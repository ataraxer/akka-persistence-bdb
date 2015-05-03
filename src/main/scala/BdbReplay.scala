package akka.persistence.journal.bdb

import java.nio.ByteBuffer

import akka.persistence.PersistentRepr

import com.sleepycat.je._

import scala.annotation.tailrec
import scala.concurrent.Future


trait BdbReplay {
  this: BdbJournal =>

  import BdbClient._

  private[this] lazy implicit val replayDispatcher = {
    context.system.dispatchers.lookup(config.getString("replay-dispatcher"))
  }


  private[this] def bytesToPersistentRepr(bytes: Array[Byte]): PersistentRepr = {
    serialization.deserialize(bytes, classOf[PersistentRepr]).get
  }


  @tailrec
  private[this] def scanFlags(cursor: Cursor, p: PersistentRepr): PersistentRepr = {
    cursor.nextValue() match {
      case BdbSuccess(entry) => {
        val value = entry.value.getData

        value.head match {
          case DataMagicByte =>
            throw new IllegalStateException(
              "Possible corrupt db, data value after first dup key.")

          case ConfirmMagicByte =>
            val cvalue = new String(value, 1, value.size - 1, "UTF-8")
            scanFlags(cursor, p.update(confirms = p.confirms :+ cvalue))

          case DeleteMagicByte =>
            scanFlags(cursor, p.update(deleted = true))
        }
      }

      case _ => p
    }
  }


  def asyncReplayMessages(
    persistenceId: String,
    fromSequenceNr: Long,
    toSequenceNr: Long,
    max: Long)
    (replayCallback: PersistentRepr => Unit): Future[Unit] =
  {
    @tailrec
    def replay(
      cursor: Cursor,
      persistenceId: String,
      count: Long)
    {
      val BdbSuccess(entry) = cursor.getCurrentKey()

      val rangeCheck = {
        keyRangeCheck(
          entry.key,
          persistenceId,
          fromSequenceNr,
          toSequenceNr)
      }

      if (rangeCheck && count < max) {
        val value = entry.value.getData

        if (value.head == DataMagicByte) {
          val data = ByteBuffer.allocate(value.size - 1)
          ByteBuffer.wrap(value, 1, value.size - 1).get(data.array)
          val persist = bytesToPersistentRepr(data.array)
          replayCallback(scanFlags(cursor, persist))
        }

        if (cursor.nextKey().isSuccess) {
          replay(cursor, persistenceId, count + 1)
        }
      }
    }


    Future {
      db withTransactionalCursor { cursor =>
        if (cursor.findKey(keyFor(persistenceId, fromSequenceNr)).isSuccess) {
          replay(cursor, persistenceId, 0L)
        }
      }
    }
  }


  def asyncReadHighestSequenceNr(persistenceId: String, fromSequenceNr: Long): Future[Long] = {
    Future {
      db withTransaction { implicit tx =>
        val operation = db.getKey(maxSeqnoKeyFor(persistenceId))

        operation map { entry =>
          ByteBuffer.wrap(entry.value.getData).getLong
        } getOrElse 0L
      }
    }
  }
}

