import gleam/bytes_builder
import gleam/erlang/process
import gleam/int
import gleam/option.{type Option, None, Some}
import gleam/otp/actor.{type Next}
import gleam/string

import glisten.{type Connection, type Message, Packet, User}

import redis/resp
import redis/store.{type Store}

type Command {
  Ping
  Echo(value: String)
  Set(key: String, value: resp.RespData, expiry: Option(Int))
  Get(key: String)
}

type CommandError {
  UnexpectedArguments(command: String, arguments: List(resp.RespData))
  UnknownCommand(command: String)
}

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
    Packet(data) -> handle_message(data, state, conn)
    User(_) -> actor.continue(state)
  }
}

fn handle_message(
  msg: BitArray,
  state: State,
  conn: Connection(a),
) -> Next(Message(a), State) {
  // Here we assume that:
  // - the whole thing is sent in a single message
  // - there is no extra content
  // - the message is always a RESP array
  // - the first element of the top-level array is always a string
  let assert Ok(resp.Parsed(data, <<>>)) = resp.parse(msg)
  let assert resp.Array([resp.String(command), ..arguments]) = data

  case parse_command(command, arguments) {
    Ok(command) -> handle_command(command, state, conn)
    Error(error) -> handle_command_error(error)
  }
}

fn parse_command(
  command: String,
  arguments: List(resp.RespData),
) -> Result(Command, CommandError) {
  case string.lowercase(command), arguments {
    "ping", [] -> Ok(Ping)
    "echo", [resp.String(value)] -> Ok(Echo(value))
    "set", [resp.String(key), value] -> Ok(Set(key, value, None))
    "set", [resp.String(key), value, resp.String(px), resp.String(expiry)] ->
      case string.lowercase(px) {
        "px" -> {
          let assert Ok(expiry) = int.parse(expiry)
          Ok(Set(key, value, Some(expiry)))
        }
        _ -> Error(UnexpectedArguments(command, arguments))
      }
    "get", [resp.String(key)] -> Ok(Get(key))

    "ping", _ -> Error(UnexpectedArguments(command, arguments))
    "echo", _ -> Error(UnexpectedArguments(command, arguments))
    "set", _ -> Error(UnexpectedArguments(command, arguments))
    "get", _ -> Error(UnexpectedArguments(command, arguments))

    _, _ -> Error(UnknownCommand(command))
  }
}

fn handle_command(
  command: Command,
  state: State,
  conn: Connection(a),
) -> Next(Message(a), State) {
  case command {
    Ping -> {
      let pong = "+PONG\r\n"
      let assert Ok(_) = glisten.send(conn, bytes_builder.from_string(pong))

      actor.continue(state)
    }

    Echo(value) -> {
      let response = "+" <> value <> "\r\n"
      let assert Ok(_) = glisten.send(conn, bytes_builder.from_string(response))

      actor.continue(state)
    }

    Set(key, value, expiry) -> {
      let state = store.upsert(state, key, value, expiry)

      let ok = "+OK\r\n"
      let assert Ok(_) = glisten.send(conn, bytes_builder.from_string(ok))

      actor.continue(state)
    }

    Get(key) -> {
      let response = case store.get(state, key) {
        Ok(value) -> resp.encode(value)
        Error(_) -> resp.encode(resp.Null)
      }

      let assert Ok(_) =
        glisten.send(conn, bytes_builder.from_bit_array(response))

      actor.continue(state)
    }
  }
}

fn handle_command_error(error: CommandError) {
  case error {
    UnexpectedArguments(command, arguments) -> {
      panic as {
        "Unexpected arguments for command "
        <> command
        <> ": "
        <> string.inspect(arguments)
      }
    }

    UnknownCommand(command) -> {
      panic as { "Unknown command: " <> command }
    }
  }
}
