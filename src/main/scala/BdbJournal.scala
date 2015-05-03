package akka.persistence.journal.bdb

import java.io.File
import java.nio.ByteBuffer

import akka.actor.Actor
import akka.persistence.journal.SyncWriteJournal
import akka.persistence.{PersistentConfirmation, PersistentId, PersistentRepr}
import akka.serialization.SerializationExtension

import com.sleepycat.je._

import scala.annotation.tailrec
import scala.collection.mutable
import scala.collection.immutable.Seq


trait BdbEnvironment extends Actor {
  private[bdb] val config = context.system.settings.config.getConfig("bdb-journal")


  private[bdb] val env = {
    import EnvironmentConfig._

    val journalDir = new File(config.getString("dir"))

    journalDir.mkdirs()

    val envConfig = {
      new EnvironmentConfig()
      .setAllowCreate(true)
      .setTransactional(true)
      .setLocking(true)
      .setConfigParam(CLEANER_THREADS, config.getString("cleaner-threads"))
      .setConfigParam(ENV_DUP_CONVERT_PRELOAD_ALL, "false")
      .setConfigParam(LOG_GROUP_COMMIT_INTERVAL, config.getString("group-commit-interval"))
      .setConfigParam(STATS_COLLECT, config.getString("stats-collect"))
      .setConfigParam(MAX_MEMORY_PERCENT, config.getString("cache-size-percent"))
    }

    new Environment(journalDir, envConfig)
  }


  override def postStop(): Unit = {
    env.close()
    super.postStop()
  }
}


class BdbJournal
  extends SyncWriteJournal
  with BdbEnvironment
  with BdbReplay
{
  import BdbClient._

  val serialization = SerializationExtension(context.system)


  private[bdb] implicit val txConfig = {
    new TransactionConfig()
    .setDurability(
      if (config.getBoolean("sync")) Durability.COMMIT_SYNC
      else Durability.COMMIT_WRITE_NO_SYNC)
    .setReadCommitted(true)
  }


  private[bdb] val db = {
    val dbConfig = {
      new DatabaseConfig()
      .setAllowCreate(true)
      .setTransactional(true)
      .setSortedDuplicates(true)
    }

    env.openDatabase(NoTransaction, "journal", dbConfig)
  }


  override def postStop(): Unit = {
    db.close()
    super.postStop()
  }


  private[bdb] def bdbSerialize(persistent: PersistentRepr): DatabaseEntry = {
    val payload = serialization.serialize(persistent).get
    val buffer = ByteBuffer.allocate(payload.size + 1)
    buffer.put(DataMagicByte)
    buffer.put(payload)
    new DatabaseEntry(buffer.array)
  }


  private[bdb] def maxSeqnoKeyFor(persistenceId: String) = keyFor(persistenceId, 0L)


  private[bdb] def keyFor(persistenceId: String, sequenceNo: Long): DatabaseEntry = {
    val persitenceIdBytes = persistenceId.getBytes("UTF-8")
    val buffer = ByteBuffer.allocate(4 + 8 + persitenceIdBytes.size)
    buffer.putInt(persitenceIdBytes.size)
    buffer.put(persitenceIdBytes)
    buffer.putLong(sequenceNo)
    new DatabaseEntry(buffer.array)
  }


  def writeMessages(messages: Seq[PersistentRepr]): Unit = {
    val maxSeqNo = mutable.Map.empty[String, Long].withDefaultValue(-1L)

    db withTransaction { implicit tx =>
      messages foreach { m =>
        val pid = m.persistenceId
        val operation = db.putKey(keyFor(pid, m.sequenceNr), bdbSerialize(m))
        maxSeqNo += (pid -> m.sequenceNr)

        if (operation.hasFailed) {
          throw new IllegalStateException("Failed to write message to database")
        }
      }

      for ((pid, max) <- maxSeqNo) {
        val key = maxSeqnoKeyFor(pid)
        val entry = new DatabaseEntry(ByteBuffer.allocate(8).putLong(max).array)
        db.deleteKey(key)

        if (db.putKey(key, entry).hasFailed) {
          throw new IllegalStateException("Failed to write maxSeqno entry to database.")
        }
      }
    }
  }


  /*
   * TODO: @deprecated remove after Akka 2.4.0 release
   */
  def writeConfirmations(confirmations: Seq[PersistentConfirmation]): Unit = {
    db withTransaction { implicit tx =>
      confirmations foreach { c =>
        val cid = c.channelId.getBytes("UTF-8")

        val buffer = ByteBuffer.allocate(1 + cid.size)
        buffer.put(ConfirmMagicByte)
        buffer.put(cid)
        val entry = new DatabaseEntry(buffer.array)

        val operation = db.putKey(keyFor(c.persistenceId, c.sequenceNr), entry)

        if (operation.hasFailed) {
          throw new IllegalStateException("Failed to write confirmation to database.")
        }
      }
    }
  }


  /*
   * TODO: @deprecated remove after Akka 2.4.0 release
   */
  def deleteMessages(messageIds: Seq[PersistentId], permanent: Boolean): Unit = {
    db withTransaction { implicit tx =>
      messageIds foreach { m =>
        db.deleteKey(keyFor(m.persistenceId, m.sequenceNr), permanent)
      }
    }
  }


  private[bdb] def keyRangeCheck(
    entry: DatabaseEntry,
    persistenceId: String,
    minSeqno: Long,
    maxSeqno: Long): Boolean =
  {
    val buf = ByteBuffer.wrap(entry.getData)
    val pidSize = buf.getInt
    val pidBytes = new Array[Byte](pidSize)
    buf.get(pidBytes)
    val pid = new String(pidBytes, "UTF-8")
    val sno = buf.getLong
    persistenceId == pid && sno >= minSeqno && sno <= maxSeqno
  }


  def deleteMessagesTo(
    persistenceId: String,
    toSequenceNr: Long,
    permanent: Boolean): Unit =
  {
    @tailrec
    def iterateCursor(cursor: Cursor): Unit = {
      val BdbSuccess(entry) = cursor.getCurrentKey()

      if (keyRangeCheck(entry.key, persistenceId, 1L, toSequenceNr)) {
        cursor.deleteKey(entry.key, permanent)
        if (cursor.nextKey().isSuccess) {
          iterateCursor(cursor)
        }
      }
    }

    db withTransactionalCursor { cursor =>
      if (cursor.findKey(keyFor(persistenceId, 1L)).isSuccess) {
        iterateCursor(cursor)
      }
    }
  }
}

