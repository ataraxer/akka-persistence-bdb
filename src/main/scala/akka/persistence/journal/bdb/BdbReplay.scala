package akka.persistence.journal.bdb

import java.nio.ByteBuffer

import akka.persistence.PersistentRepr
import akka.persistence.journal.SyncWriteJournal
import com.sleepycat.je._

import scala.annotation.tailrec
import scala.concurrent.Future


trait BdbReplay {
  this: BdbJournal with SyncWriteJournal =>

  private[this] lazy implicit val replayDispatcher = {
    context.system.dispatchers.lookup(config.getString("replay-dispatcher"))
  }


  private[this] def bytesToPersistentRepr(bytes: Array[Byte]): PersistentRepr = {
    serialization.deserialize(bytes, classOf[PersistentRepr]).get
  }


  def asyncReplayMessages(
    persistenceId: String,
    fromSequenceNr: Long,
    toSequenceNr: Long,
    max: Long)
    (replayCallback: PersistentRepr => Unit): Future[Unit] =
  {
    val pid = getPersistenceId(persistenceId)

    @tailrec
    def replay(
      tx: Transaction,
      cursor: Cursor,
      persistenceId: Long,
      count: Long)
      (replayCallback: PersistentRepr => Unit): Unit =
    {
      @tailrec
      def scanFlags(p: PersistentRepr): PersistentRepr = {
        val dbKey = new DatabaseEntry
        val dbVal = new DatabaseEntry

        val operationStatus = cursor.getNextDup(dbKey, dbVal, LockMode.DEFAULT)

        if (operationStatus == OperationStatus.SUCCESS) {
          val value = dbVal.getData
          value.head match {
            case DataMagicByte =>
              throw new IllegalStateException(
                "Possible corrupt db, data value after first dup key.")

            case ConfirmMagicByte =>
              val cvalue = new String(value, 1, value.size - 1, "UTF-8")
              scanFlags(p.update(confirms = p.confirms :+ cvalue))

            case DeleteMagicByte =>
              scanFlags(p.update(deleted = true))
          }

        } else p
      }

      val dbKey = new DatabaseEntry
      val dbVal = new DatabaseEntry

      cursor.getCurrent(dbKey, dbVal, LockMode.DEFAULT)

      val rangeCheck = keyRangeCheck(
        dbKey,
        persistenceId,
        fromSequenceNr,
        toSequenceNr)

      if (rangeCheck && count < max) {
        val value = dbVal.getData

        if (value.head == DataMagicByte) {
          val data = ByteBuffer.allocate(value.size - 1)
          ByteBuffer.wrap(value, 1, value.size - 1).get(data.array)
          val persist = bytesToPersistentRepr(data.array)

          replayCallback(scanFlags(persist))
        }

        val operationStatus = cursor.getNextNoDup(dbKey, dbVal, LockMode.DEFAULT)

        if (operationStatus == OperationStatus.SUCCESS) {
          replay(tx, cursor, persistenceId, count + 1)(replayCallback)
        }
      }
    }


    Future {
      withTransactionalCursor(db) { (cursor, tx) =>
        val operationStatus = cursor.getSearchKeyRange(
          getKey(pid, fromSequenceNr),
          new DatabaseEntry,
          LockMode.DEFAULT)

        if (operationStatus == OperationStatus.SUCCESS) {
          replay(tx, cursor, pid, 0L)(replayCallback)
        }
      }
    }
  }


  def asyncReadHighestSequenceNr(persistenceId: String, fromSequenceNr: Long): Future[Long] = {
    Future {
      withTransaction { tx =>
        val pid = getPersistenceId(persistenceId)
        val dbVal = new DatabaseEntry
        val operationStatus = db.get(tx, getMaxSeqnoKey(pid), dbVal, LockMode.DEFAULT)

        if (operationStatus == OperationStatus.SUCCESS) {
          ByteBuffer.wrap(dbVal.getData).getLong
        } else 0L
      }
    }
  }
}
