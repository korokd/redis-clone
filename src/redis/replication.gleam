import gleam/io

import gleam/bytes_builder
import gleam/erlang/process.{type Subject}
import gleam/int
import gleam/list
import gleam/otp/actor.{type Next}
import gleam/result

import glisten.{type Connection, type SocketReason}

import redis/command.{type Command}
import redis/resp

pub type Replication =
  Subject(ReplicationMessage)

pub type ReplicationData {
  ReplicationData(
    replid: String,
    repl_offset: Int,
    replicas_conns: List(Connection(BitArray)),
  )
}

pub opaque type ReplicationMessage {
  AddReplica(Connection(BitArray))
  GetReplicationData(Subject(ReplicationData))
}

pub fn init() -> Replication {
  let replid = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
  let repl_offset = 0

  let assert Ok(replication) =
    actor.start(
      ReplicationData(replid, repl_offset, replicas_conns: []),
      handle_replication,
    )

  replication
}

fn handle_replication(
  msg: ReplicationMessage,
  state: ReplicationData,
) -> Next(ReplicationMessage, ReplicationData) {
  case msg {
    AddReplica(conn) ->
      ReplicationData(..state, replicas_conns: [conn, ..state.replicas_conns])
      |> actor.continue()

    GetReplicationData(subject) -> {
      process.send(subject, state)

      actor.continue(state)
    }
  }
}

pub fn add_replica(replication: Replication, conn: Connection(BitArray)) -> Nil {
  process.send(replication, AddReplica(conn))
}

pub fn get_replication_data(replication: Replication) -> ReplicationData {
  process.call(replication, GetReplicationData, 10)
}

pub fn propagate(
  replication: Replication,
  command: Command,
) -> Result(Nil, SocketReason) {
  let msg =
    command.to_resp_data(command)
    |> resp.encode()

  get_replication_data(replication).replicas_conns
  |> list.map(fn(conn) { glisten.send(conn, bytes_builder.from_bit_array(msg)) })
  |> result.all()
  |> result.map(fn(_) { Nil })
}

pub fn get_info(replication: Replication) -> List(String) {
  let ReplicationData(replid, repl_offset, _) =
    get_replication_data(replication)

  [
    "role:master",
    "master_replid:" <> replid,
    "master_repl_offset:" <> int.to_string(repl_offset),
  ]
}
