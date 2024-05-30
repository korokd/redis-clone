import gleam/io

import gleam/bytes_builder
import gleam/erlang/process
import gleam/int
import gleam/option.{None}
import gleam/otp/actor.{type Next}
import gleam/string

import glisten.{type Connection, type Message}

import redis/command.{type Command, type CommandError}
import redis/config
import redis/resp
import redis/state.{type State}
import redis/store

const empty_rdb_file_in_base_64 = "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog=="

pub fn main() {
  let state = state.init()
  let port =
    state.get_config(state)
    |> config.get_port()

  let assert Ok(_) =
    glisten.handler(fn(_conn) { #(state, None) }, loop)
    |> glisten.serve(port)

  process.sleep_forever()
}

fn loop(
  msg: Message(a),
  state: State,
  conn: Connection(a),
) -> Next(Message(a), State) {
  case msg {
    glisten.Packet(data) -> handle_message(data, state, conn)
    glisten.User(_) -> actor.continue(state)
  }
}

fn handle_message(
  msg: BitArray,
  state: State,
  conn: Connection(a),
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
  conn: Connection(a),
) -> Next(Message(a), State) {
  case command {
    command.RDBFile(_) -> {
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
      let state =
        state.get_store(state)
        |> store.upsert(key, value, expiry)
        |> state.update_store(state)

      let assert Ok(_) =
        glisten.send(conn, bytes_builder.from_bit_array(response))

      actor.continue(state)
    }

    command.Get(key) -> {
      let response = case store.get(state.get_store(state), key) {
        Ok(value) -> resp.encode(value)
        Error(_) -> resp.encode(resp.Null)
      }

      let assert Ok(_) =
        glisten.send(conn, bytes_builder.from_bit_array(response))

      actor.continue(state)
    }

    command.Info(command.Replication) -> {
      let response =
        state.get_config(state)
        |> config.get_replication_info()
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
      let full_resync = case state.get_config(state) {
        config.Master(_, replid, repl_offset) ->
          resp.encode(resp.SimpleString(
            "FULLRESYNC " <> replid <> " " <> int.to_string(repl_offset),
          ))

        config.Replica(_, _) -> resp.encode(resp.Null)
      }

      let assert Ok(_) =
        glisten.send(conn, bytes_builder.from_bit_array(full_resync))

      let file = resp.encode(resp.RDBFile(empty_rdb_file_in_base_64))

      let assert Ok(_) = glisten.send(conn, bytes_builder.from_bit_array(file))

      actor.continue(state)
    }
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
