package akka.persistence.journal.bdb

import com.sleepycat.je._


object BdbClient {
  val DataMagicByte = 0x0.toByte
  val ConfirmMagicByte = 0x1.toByte
  val DeleteMagicByte = 0x2.toByte

  def deletedEntry = new DatabaseEntry(Array(DeleteMagicByte))

  val NoTransaction = null.asInstanceOf[Transaction]
  val NoTransactionConfig = null.asInstanceOf[TransactionConfig]


  trait BdbOps extends Any {
    def deleteKey(key: DatabaseEntry, permanent: Boolean): OperationStatus
    def putKey(key: DatabaseEntry, data: DatabaseEntry): OperationStatus
    def getKey(key: DatabaseEntry, data: DatabaseEntry, lockMode: LockMode): OperationStatus
  }


  implicit class RichCursor(val cursor: Cursor) extends AnyVal with BdbOps {
    def deleteKey(key: DatabaseEntry, permanent: Boolean = true) = {
      if (permanent) cursor.delete() else cursor.put(key, deletedEntry)
    }

    def putKey(key: DatabaseEntry, data: DatabaseEntry) = {
      cursor.put(key, data)
    }

    def getKey(key: DatabaseEntry, data: DatabaseEntry, lockMode: LockMode) = {
      cursor.getCurrent(key, data, lockMode)
    }
  }


  implicit class TransactionalDatabase(db: Database)(implicit tx: Transaction) extends BdbOps {
    def deleteKey(key: DatabaseEntry, permanent: Boolean = true) = {
      if (permanent) db.delete(tx, key) else db.put(tx, key, deletedEntry)
    }

    def putKey(key: DatabaseEntry, data: DatabaseEntry) = {
      db.put(tx, key, data)
    }

    def getKey(key: DatabaseEntry, data: DatabaseEntry, lockMode: LockMode) = {
      db.get(tx, key, data, lockMode)
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

