package akka.persistence.journal.bdb

import akka.actor._
import akka.persistence._
import com.typesafe.config._


final class BdbPersistenceSettings(config: Config) {

}


object BdbPersistence
  extends ExtensionId[BdbPersistence] {

  def createExtension(system: ExtendedActorSystem) = {
    new BdbPersistence(system)
  }

  def lookup() = BdbPersistence
}


class BdbPersistence(val system: ExtendedActorSystem)
  extends Extension {

  private lazy val config = {
    system.settings.config.getConfig("bdb-persistence")
  }

  private lazy val replicatedEnvironment = BdbEnvironment(config)

  def isMaster = replicatedEnvironment.isMaster

  def isReplicated = replicatedEnvironment.isReplicated

  def openDatabase(name: String) = replicatedEnvironment.openDatabase(name)
}

