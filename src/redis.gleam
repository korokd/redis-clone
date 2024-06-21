import gleam/io

import gleam/bytes_builder
import gleam/erlang/process
import gleam/int
import gleam/option.{None}
import gleam/otp/actor.{type Next}
import gleam/string

import glisten.{type Connection, type Message, type SocketReason}

import redis/command.{type Command, type CommandError}
import redis/config.{type Config}
import redis/replication
import redis/resp
import redis/store.{type Store}

const empty_rdb_file_in_base_64 = "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog=="

type State {
  State(config: Config, store: Store)
}

pub fn main() {
  let store = store.init()
  let config = config.init(store)
  let port = config.port

  let state = State(config, store)

  let assert Ok(_) =
    glisten.handler(fn(_conn) { #(state, None) }, loop)
    |> glisten.serve(port)

  io.println("Listening on port " <> int.to_string(port))
  process.sleep_forever()
}

fn loop(
  msg: Message(a),
  state: State,
  conn: Connection(BitArray),
) -> Next(Message(a), State) {
  case msg {
    glisten.Packet(data) -> handle_message(data, state, conn)
    glisten.User(_) -> actor.continue(state)
  }
}

fn handle_message(
  msg: BitArray,
  state: State,
  conn: Connection(BitArray),
) -> Next(Message(a), State) {
  let assert Ok(data) = resp.parse(msg)

  case command.from_resp_data(data) {
    Ok(command) -> handle_command(command, state, conn)

    Error(error) -> handle_command_error(error)
  }
}

fn handle_command(
  command: Command,
  state: State,
  conn: Connection(BitArray),
) -> Next(Message(a), State) {
  case command {
    command.Binary(_) -> {
      actor.continue(state)
    }

    command.Ping -> {
      let response = resp.encode(resp.SimpleString("PONG"))

      let assert Ok(_) =
        glisten.send(conn, bytes_builder.from_bit_array(response))

      actor.continue(state)
    }

    command.Echo(value) -> {
      let response = resp.encode(value)

      let assert Ok(_) =
        glisten.send(conn, bytes_builder.from_bit_array(response))

      actor.continue(state)
    }

    command.Set(key, value, expiry) -> {
      let response = resp.encode(resp.SimpleString("OK"))
      store.upsert(state.store, key, value, expiry)

      let assert Ok(_) =
        glisten.send(conn, bytes_builder.from_bit_array(response))

      // The Master does not wait for the Replicas to respond
      let _propagated = propagate_if_master(command, state)

      actor.continue(state)
    }

    command.Get(key) -> {
      let response = case store.get(state.store, key) {
        Ok(value) -> resp.encode(value)
        Error(_) -> resp.encode(resp.Null)
      }

      let assert Ok(_) =
        glisten.send(conn, bytes_builder.from_bit_array(response))

      actor.continue(state)
    }

    command.Info(command.Replication) -> {
      let response =
        config.get_info(state.config)
        |> string.join("\r\n")
        |> resp.BulkString()
        |> resp.encode()

      let assert Ok(_) =
        glisten.send(conn, bytes_builder.from_bit_array(response))

      actor.continue(state)
    }

    command.ReplConf(_) -> {
      let response = resp.encode(resp.SimpleString("OK"))

      let assert Ok(_) =
        glisten.send(conn, bytes_builder.from_bit_array(response))

      actor.continue(state)
    }

    command.Psync(_, _) -> {
      case state.config {
        config.Master(_port, master) -> {
          let replication.ReplicationData(replid, repl_offset, _) =
            replication.get_replication_data(master)

          let response =
            resp.encode(resp.SimpleString(
              "FULLRESYNC " <> replid <> " " <> int.to_string(repl_offset),
            ))

          let assert Ok(_) =
            glisten.send(conn, bytes_builder.from_bit_array(response))

          let file = resp.encode(resp.Binary(empty_rdb_file_in_base_64))

          let assert Ok(_) =
            glisten.send(conn, bytes_builder.from_bit_array(file))

          replication.add_replica(master, conn)

          actor.continue(state)
        }

        config.Replica(_port, _replica) -> {
          let response = resp.encode(resp.Null)

          let assert Ok(_) =
            glisten.send(conn, bytes_builder.from_bit_array(response))

          actor.continue(state)
        }
      }
    }
  }
}

fn propagate_if_master(
  command: Command,
  state: State,
) -> Result(Nil, SocketReason) {
  case state.config {
    config.Replica(_port, _replica) -> Ok(Nil)

    config.Master(_port, master) -> replication.propagate(master, command)
  }
}

fn handle_command_error(error: CommandError) {
  case error {
    command.UnexpectedArguments(command, arguments) -> {
      panic as {
        "Unexpected arguments for command "
        <> command
        <> ": "
        <> string.inspect(arguments)
      }
    }

    command.UnknownCommand(command) -> {
      panic as { "Unknown command: " <> command }
    }
  }
}
