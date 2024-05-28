import gleam/bytes_builder
import gleam/erlang/process
import gleam/option.{None}
import gleam/otp/actor.{type Next}
import gleam/string

import glisten.{type Connection, type Message}

import redis/command.{type Command, type CommandError}
import redis/resp
import redis/store.{type Store}

type State =
  Store

pub fn main() {
  let state: State = store.new()

  let assert Ok(_) =
    glisten.handler(fn(_conn) { #(state, None) }, loop)
    |> glisten.serve(6379)

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
  let state = case command {
    command.Ping -> {
      let pong = "+PONG\r\n"
      let assert Ok(_) = glisten.send(conn, bytes_builder.from_string(pong))

      state
    }

    command.Echo(value) -> {
      let response = "+" <> value <> "\r\n"
      let assert Ok(_) = glisten.send(conn, bytes_builder.from_string(response))

      state
    }

    command.Set(key, value, expiry) -> {
      let state = store.upsert(state, key, value, expiry)

      let ok = "+OK\r\n"
      let assert Ok(_) = glisten.send(conn, bytes_builder.from_string(ok))

      state
    }

    command.Get(key) -> {
      let response = case store.get(state, key) {
        Ok(value) -> resp.encode(value)
        Error(_) -> resp.encode(resp.Null)
      }

      let assert Ok(_) =
        glisten.send(conn, bytes_builder.from_bit_array(response))

      state
    }
  }

  actor.continue(state)
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
