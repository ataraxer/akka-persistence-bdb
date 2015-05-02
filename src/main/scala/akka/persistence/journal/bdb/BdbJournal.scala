package akka.persistence.journal.bdb

import java.io.File
import java.nio.ByteBuffer

import akka.actor.Actor
import akka.persistence.journal.SyncWriteJournal
import akka.persistence.{PersistentConfirmation, PersistentId, PersistentRepr}
import akka.serialization.SerializationExtension

import com.sleepycat.je._

import scala.annotation.tailrec
import scala.collection.immutable.Seq


trait BdbEnvironment extends Actor {
  val config = context.system.settings.config.getConfig("bdb-journal")
  val journalDir = new File(config.getString("dir"))
  journalDir.mkdirs()


  val envConfig = {
    import EnvironmentConfig._

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


  val env = new Environment(journalDir, envConfig)


  override def postStop(): Unit = {
    env.close()
    super.postStop()
  }
}


class BdbJournal
  extends SyncWriteJournal
  with BdbEnvironment
  with BdbKeys
  with BdbReplay
{
  private[bdb] final val DataMagicByte = 0x0.toByte
  private[bdb] final val ConfirmMagicByte = 0x1.toByte
  private[bdb] final val DeleteMagicByte = 0x2.toByte

  val serialization = SerializationExtension(context.system)


  private[bdb] val dbConfig = {
    new DatabaseConfig()
    .setAllowCreate(true)
    .setTransactional(true)
    .setSortedDuplicates(true)
  }


  private[bdb] val txConfig = {
    new TransactionConfig()
    .setDurability(
      if (config.getBoolean("sync")) Durability.COMMIT_SYNC
      else Durability.COMMIT_WRITE_NO_SYNC)
    .setReadCommitted(true)
  }


  private[bdb] val db = env.openDatabase(null, "journal", dbConfig)


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


  def writeMessages(messages: Seq[PersistentRepr]): Unit = {
    var max = Map.empty[Long, Long].withDefaultValue(-1L)

    withTransaction { tx =>
      messages foreach { m =>
        val pid = getPersistenceId(m.processorId)

        val operationStatus = db.put(tx, getKey(pid, m.sequenceNr), bdbSerialize(m))

        if (operationStatus != OperationStatus.SUCCESS) {
          throw new IllegalStateException("Failed to write message to database")
        }

        if (max(pid) < m.sequenceNr) {
          max += (pid -> m.sequenceNr)
        }
      }

      for ((p, m) <- max) {
        val key = getMaxSeqnoKey(p)
        db.delete(tx, key)
        val entry = new DatabaseEntry(ByteBuffer.allocate(8).putLong(m).array)

        if (db.put(tx, key, entry) != OperationStatus.SUCCESS) {
          throw new IllegalStateException("Failed to write maxSeqno entry to database.")
        }
      }
    }
  }


  def writeConfirmations(confirmations: Seq[PersistentConfirmation]): Unit = {
    withTransaction { tx =>
      confirmations foreach { c =>
        val cid = c.channelId.getBytes("UTF-8")

        val entry = new DatabaseEntry(
          ByteBuffer
          .allocate(cid.size + 1)
          .put(ConfirmMagicByte)
          .put(cid)
          .array)

        val operationStatus = db.put(
          tx,
          getKey(c.processorId, c.sequenceNr),
          entry)

        if (operationStatus != OperationStatus.SUCCESS) {
          throw new IllegalStateException("Failed to write confirmation to database.")
        }
      }
    }
  }


  def deleteMessages(messageIds: Seq[PersistentId], permanent: Boolean): Unit = {
    withTransaction { tx =>
      messageIds foreach { m =>
        deleteKey(tx, getKey(m.processorId, m.sequenceNr), permanent)
      }
    }
  }


  private[bdb] def keyRangeCheck(
    entry: DatabaseEntry,
    processorId: Long,
    minSeqno: Long,
    maxSeqno: Long): Boolean =
  {
    val buf = ByteBuffer.wrap(entry.getData)
    val pid = buf.getLong
    val sno = buf.getLong
    processorId == pid && sno >= minSeqno && sno <= maxSeqno
  }


  def deleteMessagesTo(
    processorId: String,
    toSequenceNr: Long,
    permanent: Boolean): Unit =
  {
    @tailrec
    def iterateCursor(cursor: Cursor, processorId: Long): Unit = {
      val dbKey = new DatabaseEntry
      val dbVal = new DatabaseEntry

      cursor.getCurrent(dbKey, dbVal, LockMode.DEFAULT)

      if (keyRangeCheck(dbKey, processorId, 1L, toSequenceNr)) {
        deleteKey(cursor, dbKey, permanent)
        if (cursor.getNextNoDup(dbKey, dbVal, LockMode.DEFAULT) == OperationStatus.SUCCESS)
          iterateCursor(cursor, processorId)
      }
    }

    withTransactionalCursor(db) { (cursor, tx) =>
      val operationStatus = {
        cursor.getSearchKeyRange(
        getKey(processorId, 1L),
        new DatabaseEntry,
        LockMode.DEFAULT)
      }

      if (operationStatus == OperationStatus.SUCCESS) {
        iterateCursor(cursor, getPersistenceId(processorId))
      }
    }
  }


  private[this] def deleteKey(
    cursor: Cursor,
    key: DatabaseEntry,
    permanent: Boolean): Unit =
  {
    if (permanent) {
      cursor.delete()
    } else {
      cursor.put(key, new DatabaseEntry(Array(DeleteMagicByte)))
    }
  }


  private[this] def deleteKey(
    tx: Transaction,
    key: DatabaseEntry,
    permanent: Boolean): Unit =
  {
    if (permanent) {
      db.delete(tx, key)
    } else {
      db.put(tx, key, new DatabaseEntry(Array(DeleteMagicByte)))
    }
  }


  private[bdb] def withTransaction[T](p: Transaction => T): T = {
    val tx = env.beginTransaction(null, txConfig)
    try p(tx) finally cleanupTx(tx)
  }


  private[bdb] def withTransactionalCursor[T]
    (db: Database)
    (p: (Cursor, Transaction) => T): Unit =
  {
    val tx = env.beginTransaction(null, txConfig)
    val cursor = db.openCursor(tx, CursorConfig.READ_COMMITTED)
    try {
      p(cursor, tx)
    } finally {
      cursor.close()
      cleanupTx(tx)
    }
  }


  private[bdb] def cleanupTx(tx: Transaction): Unit = {
    if (tx.isValid) tx.commit() else tx.abort()
  }
}

