package akka.persistence.journal.bdb

import com.sleepycat.je._


object BdbClient {
  val DataMagicByte = 0x0.toByte
  val ConfirmMagicByte = 0x1.toByte
  val DeleteMagicByte = 0x2.toByte

  def ConfirmFlag = new DatabaseEntry(Array(ConfirmMagicByte))
  def DeleteFlag = new DatabaseEntry(Array(DeleteMagicByte))

  val NoTransaction = null.asInstanceOf[Transaction]
  val NoTransactionConfig = null.asInstanceOf[TransactionConfig]

  type BdbKey = DatabaseEntry
  type BdbValue = DatabaseEntry
  case class BdbEntry(key: BdbKey, value: BdbValue)


  implicit class RichCursor(val cursor: Cursor) extends AnyVal {
    def deleteKey(key: BdbKey, permanent: Boolean = true) = {
      val status = if (permanent) cursor.delete() else cursor.put(key, DeleteFlag)
      BdbOperation(status, key)
    }

    def putKey(key: BdbKey, data: BdbValue) = {
      BdbOperation(cursor.put(key, data), BdbEntry(key, data))
    }

    def getCurrentKey(lockMode: LockMode = LockMode.DEFAULT) = {
      val key = new DatabaseEntry
      val data = new DatabaseEntry
      BdbOperation(cursor.getCurrent(key, data, lockMode), BdbEntry(key, data))
    }

    def findKey(key: BdbKey, lockMode: LockMode = LockMode.DEFAULT) = {
      val data = new DatabaseEntry
      BdbOperation(cursor.getSearchKeyRange(key, data, lockMode), BdbEntry(key, data))
    }

    def nextValue(lockMode: LockMode = LockMode.DEFAULT) = {
      val key = new DatabaseEntry
      val data = new DatabaseEntry
      BdbOperation(cursor.getNextDup(key, data, lockMode), BdbEntry(key, data))
    }

    def nextKey(lockMode: LockMode = LockMode.DEFAULT) = {
      val key = new DatabaseEntry
      val data = new DatabaseEntry
      BdbOperation(cursor.getNextNoDup(key, data, lockMode), BdbEntry(key, data))
    }
  }


  implicit class TransactionalDatabase(val db: Database) extends AnyVal {
    def deleteKey
      (key: DatabaseEntry, permanent: Boolean = true)
      (implicit tx: Transaction) =
    {
      val status = if (permanent) db.delete(tx, key) else db.put(tx, key, DeleteFlag)
      BdbOperation(status, key)
    }

    def putKey
      (key: DatabaseEntry, data: DatabaseEntry)
      (implicit tx: Transaction) =
    {
      BdbOperation(db.put(tx, key, data), BdbEntry(key, data))
    }

    def getKey
      (key: DatabaseEntry, lockMode: LockMode = LockMode.DEFAULT)
      (implicit tx: Transaction) =
    {
      val data = new DatabaseEntry
      BdbOperation(db.get(tx, key, data, lockMode), BdbEntry(key, data))
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
    else this.asInstanceOf[BdbOperation[Nothing]]
  }

  def andThen[U](otherOperation: => BdbOperation[U]): BdbOperation[U] = {
    if (isSuccess) otherOperation
    else this.asInstanceOf[BdbOperation[Nothing]]
  }

  def map[U](f: T => U): BdbOperation[U] = {
    if (isSuccess) BdbSuccess(f(value))
    else this.asInstanceOf[BdbOperation[Nothing]]
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

