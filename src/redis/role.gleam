import gleam/io

import redis/master.{type Master}
import redis/replica.{type Replica}

pub type Role {
  Master(master: Master)
  Replica(replica: Replica)
}

pub fn init_master() -> Role {
  Master(master: master.init())
}

pub fn init_replica(
  own_port: Int,
  master_host: String,
  master_port: Int,
) -> Role {
  Replica(replica: replica.init(own_port, master_host, master_port))
}

pub fn get_info(role: Role) -> List(String) {
  case role {
    Master(master) -> master.get_info(master)

    Replica(_replica) -> replica.get_info()
  }
}
