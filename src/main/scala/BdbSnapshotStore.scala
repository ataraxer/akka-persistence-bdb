package akka.persistence.journal.bdb

import java.io.File
import java.nio.ByteBuffer

import akka.actor.Actor
import akka.persistence._
import akka.persistence.snapshot._
import akka.persistence.serialization.Snapshot
import akka.serialization.{SerializationExtension, Serialization}

import com.sleepycat.je._

import scala.annotation.tailrec
import scala.collection.mutable
import scala.collection.immutable.Seq
import scala.concurrent.Future


object BdbSnapshotStore {
  import BdbClient._

  def writeMetadata(metadata: SnapshotMetadata): DatabaseEntry = {
    val persistenceIdBytes = metadata.persistenceId.getBytes("UTF-8")
    val buffer = ByteBuffer.allocate(4 + persistenceIdBytes.size + 8 + 8)
    buffer.putInt(persistenceIdBytes.size)
    buffer.put(persistenceIdBytes)
    buffer.putLong(metadata.sequenceNr)
    buffer.putLong(metadata.timestamp)
    new DatabaseEntry(buffer.array)
  }


  def readMetadata(entry: DatabaseEntry): SnapshotMetadata = {
    val buffer = ByteBuffer.wrap(entry.getData)
    val pidSize = buffer.getInt
    val pidBytes = new Array[Byte](pidSize)
    buffer.get(pidBytes)
    val persistenceId = new String(pidBytes, "UTF-8")
    val sequenceNr = buffer.getLong
    val timestamp = buffer.getLong
    SnapshotMetadata(persistenceId, sequenceNr, timestamp)
  }


  def writeSnapshot(data: Any)(implicit serialization: Serialization): DatabaseEntry = {
    val snapshot = Snapshot(data)
    val payload = serialization.findSerializerFor(snapshot).toBinary(snapshot)
    val buffer = ByteBuffer.allocate(1 + payload.size)
    buffer.put(DataMagicByte)
    buffer.put(payload)
    new DatabaseEntry(buffer.array)
  }


  def readSnapshot(entry: DatabaseEntry)(implicit serialization: Serialization): Any = {
    val bytes = entry.getData.tail
    val snapshot = serialization.deserialize(bytes, classOf[Snapshot])
    snapshot.get.data
  }
}


class BdbSnapshotStore
  extends Actor
  with SnapshotStore
{
  import BdbSnapshotStore._
  import BdbClient._

  val config = context.system.settings.config.getConfig("bdb-snapshot-store")

  val environment = BdbPersistence(context.system)

  implicit val serialization = SerializationExtension(context.system)

  private lazy implicit val pluginDispatcher = {
    context.system.dispatchers.lookup(config.getString("plugin-dispatcher"))
  }


  private implicit val txConfig = {
    new TransactionConfig()
    .setDurability(
      if (config.getBoolean("sync")) Durability.COMMIT_SYNC
      else Durability.COMMIT_WRITE_NO_SYNC)
    .setReadCommitted(true)
  }


  private val db = {
    val dbConfig = {
      new DatabaseConfig()
      .setAllowCreate(true)
      .setTransactional(true)
      .setSortedDuplicates(true)
    }

    environment.openDatabase("snapshots")
  }


  override def postStop(): Unit = {
    db.close()
    super.postStop()
  }


  def loadAsync(
    persistenceId: String,
    criteria: SnapshotSelectionCriteria): Future[Option[SelectedSnapshot]] =
  {
    Future {
      val maxSequenceNr = criteria.maxSequenceNr
      val maxTimestamp = criteria.maxTimestamp

      @tailrec
      def iterateCursor(cursor: Cursor): Option[BdbEntry] = {
        cursor.lastValue()
        val BdbSuccess(entry) = cursor.getCurrentKey()
        val metadata = readMetadata(entry.key)

        if (metadata.persistenceId == persistenceId) {
          // actual data is stored in the first duplicate key
          // because duplicate sorting is enabled
          val dataFetch = cursor.prevValue()

          val isValid = {
            metadata.timestamp <= maxTimestamp &&
            metadata.sequenceNr <= maxSequenceNr &&
            // make sure confirmation record exists
            entry.value.getData.head == ConfirmMagicByte &&
            dataFetch.isSuccess
          }

          if (isValid) Some(dataFetch.value) else {
            if (cursor.prevKey().isSuccess) iterateCursor(cursor)
            else None
          }
        } else None
      }

      /**
       * Steps back key by key while persistence id of a database record is bigger
       * than the required one.
       */
      @tailrec
      def findPersistenceId(cursor: Cursor): Unit = {
        val BdbSuccess(entry) = cursor.getCurrentKey()
        val metadata = readMetadata(entry.key)

        if (metadata.persistenceId > persistenceId) {
          if (cursor.prevKey().isSuccess) findPersistenceId(cursor)
        }
      }

      val result = db withTransactionalCursor { cursor =>
        val metadata = SnapshotMetadata(persistenceId, maxSequenceNr, maxTimestamp)
        /*
         * Look for a key matching the criteria -- this will return the smallest
         * key which is greater or equal to criteria.
         *
         * If lookup has failed -- attempt to move back by one key to check if there is any
         * data in the database at all, so we can start looking for a snapshot within upper value.
         */
        val lookup = cursor.findKey(writeMetadata(metadata)) orElse cursor.prevKey()

        if (lookup.isSuccess) {
          findPersistenceId(cursor)
          iterateCursor(cursor)
        } else None
      }

      result map { entry =>
        SelectedSnapshot(readMetadata(entry.key), readSnapshot(entry.value))
      }
    }
  }


  def saveAsync(metadata: SnapshotMetadata, snapshot: Any): Future[Unit] = {
    Future {
      db withTransaction { implicit tx =>
        val key = writeMetadata(metadata)
        val entry = writeSnapshot(snapshot)

        if (db.putKey(key, entry).hasFailed) {
          throw new IllegalStateException("Failed to write message to database")
        }
      }
    }
  }


  def saved(metadata: SnapshotMetadata): Unit = {
    db withTransaction { implicit tx =>
      val key = writeMetadata(metadata)
      db.putKey(key, ConfirmFlag)
    }
  }


  def delete(metadata: SnapshotMetadata): Unit = {
    db withTransaction { implicit tx =>
      val key = writeMetadata(metadata)
      db.deleteKey(key, permanent = true)
    }
  }


  def delete(persistenceId: String, criteria: SnapshotSelectionCriteria): Unit = {
    val maxSequenceNr = criteria.maxSequenceNr
    val maxTimestamp = criteria.maxTimestamp

    @tailrec
    def iterateCursor(cursor: Cursor): Unit = {
      val BdbSuccess(entry) = cursor.getCurrentKey()
      val metadata = readMetadata(entry.key)

      val isValid = {
        metadata.persistenceId == persistenceId &&
        metadata.sequenceNr <= maxSequenceNr &&
        metadata.timestamp <= maxTimestamp
      }

      if (isValid) {
        cursor.deleteKey(entry.key, permanent = true)
        if (cursor.nextKey().isSuccess) iterateCursor(cursor)
      }
    }

    db withTransactionalCursor { cursor =>
      val metadata = SnapshotMetadata(persistenceId, 0L, 0L)
      if (cursor.findKey(writeMetadata(metadata)).isSuccess) {
        iterateCursor(cursor)
      }
    }
  }
}

