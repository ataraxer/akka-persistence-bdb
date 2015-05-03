package akka.persistence.journal.bdb

import com.sleepycat.je._


object BdbClient {
  val DataMagicByte = 0x0.toByte
  val ConfirmMagicByte = 0x1.toByte
  val DeleteMagicByte = 0x2.toByte

  def deletedEntry = new DatabaseEntry(Array(DeleteMagicByte))

  val NoTransaction = null.asInstanceOf[Transaction]
  val NoTransactionConfig = null.asInstanceOf[TransactionConfig]

  type BdbKey = DatabaseEntry
  type BdbValue = DatabaseEntry
  type BdbEntry = (BdbKey, BdbValue)


  trait BdbOps extends Any {
    def deleteKey(key: BdbKey, permanent: Boolean): BdbOperation[BdbKey]
    def putKey(key: DatabaseEntry, data: DatabaseEntry): BdbOperation[BdbEntry]
    def getKey(key: DatabaseEntry, lockMode: LockMode): BdbOperation[BdbEntry]
  }


  implicit class RichCursor(val cursor: Cursor) extends AnyVal with BdbOps {
    def deleteKey(key: BdbKey, permanent: Boolean = true) = {
      val status = if (permanent) cursor.delete() else cursor.put(key, deletedEntry)
      BdbOperation(status, key)
    }

    def putKey(key: BdbKey, data: BdbValue) = {
      BdbOperation(cursor.put(key, data), key -> data)
    }

    def getKey(key: BdbKey, lockMode: LockMode) = {
      // TODO: change
      val data = new DatabaseEntry
      BdbOperation(cursor.getCurrent(key, data, lockMode), key -> data)
    }

    def getCurrentKey(lockMode: LockMode) = {
      val key = new DatabaseEntry
      val data = new DatabaseEntry
      BdbOperation(cursor.getCurrent(key, data, lockMode), key -> data)
    }
  }


  implicit class TransactionalDatabase(db: Database)(implicit tx: Transaction) extends BdbOps {
    def deleteKey(key: DatabaseEntry, permanent: Boolean = true) = {
      val status = if (permanent) db.delete(tx, key) else db.put(tx, key, deletedEntry)
      BdbOperation(status, key)
    }

    def putKey(key: DatabaseEntry, data: DatabaseEntry) = {
      BdbOperation(db.put(tx, key, data), key -> data)
    }

    def getKey(key: DatabaseEntry, lockMode: LockMode) = {
      val data = new DatabaseEntry
      BdbOperation(db.get(tx, key, data, lockMode), key -> data)
    }
  }


  implicit class RichDatabase(val db: Database) extends AnyVal {
    def withTransaction[T]
      (action: Transaction => T)
      (implicit txConfig: TransactionConfig = NoTransactionConfig): T =
    {
      val tx = db.getEnvironment.beginTransaction(NoTransaction, txConfig)
      try action(tx) finally cleanupTx(tx)
    }


    def withTransactionalCursor[T]
      (action: Cursor => T)
      (implicit txConfig: TransactionConfig = NoTransactionConfig): T =
    {
      val tx = db.getEnvironment.beginTransaction(NoTransaction, txConfig)
      val cursor = db.openCursor(tx, CursorConfig.READ_COMMITTED)

      try action(cursor) finally {
        cursor.close()
        cleanupTx(tx)
      }
    }
  }


  private[bdb] def cleanupTx(tx: Transaction): Unit = {
    if (tx.isValid) tx.commit() else tx.abort()
  }
}


object BdbOperation {
  def apply[T](status: OperationStatus, result: => T): BdbOperation[T] = {
    status match {
      case OperationStatus.SUCCESS => BdbSuccess(result)
      case OperationStatus.NOTFOUND => BdbKeyNotFound
      case OperationStatus.KEYEXIST => BdbKeyExists
      case OperationStatus.KEYEMPTY => BdbKeyEmpty
    }
  }
}


trait BdbOperation[+T] {
  def isSuccess: Boolean
  def value: T

  def hasFailed = !isSuccess

  def flatMap[U](f: T => BdbOperation[U]): BdbOperation[U] = {
    if (isSuccess) f(value)
    else this.asInstanceOf[BdbOperation[U]]
  }

  def map[U](f: T => U): BdbOperation[U] = {
    if (isSuccess) BdbSuccess(f(value))
    else this.asInstanceOf[BdbOperation[U]]
  }

  def foreach(f: T => Unit) = {
    if (isSuccess) f(value)
  }

  def filter(f: T => Boolean): BdbOperation[T] = {
    if (isSuccess && f(value)) this
    else BdbPredicateFailed
  }

  def getOrElse[U >: T](fallback: => U): U = {
    if (isSuccess) value else fallback
  }

  def withFilter(f: T => Boolean) = filter(f)
}


case class BdbSuccess[T](val value: T) extends BdbOperation[T] {
  val isSuccess = true
}


trait BdbFailure extends BdbOperation[Nothing] {
  val isSuccess = false
  def value = ???
}


case object BdbPredicateFailed extends BdbFailure
case object BdbKeyEmpty extends BdbFailure
case object BdbKeyExists extends BdbFailure
case object BdbKeyNotFound extends BdbFailure

