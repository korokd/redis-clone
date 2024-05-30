import gleam/io

import gleam/bytes_builder
import gleam/erlang/process
import gleam/int
import gleam/list
import gleam/option.{None, Some}
import gleam/otp/actor.{type Next}
import gleam/string

import glisten.{type Connection, type Message}

import redis/command.{type Command, type CommandError}
import redis/resp
import redis/state.{type State}
import redis/store

pub fn main() {
  let state = state.init()
  let port = state.get_port(state)

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
  let #(response, state) = case command {
    command.Ping -> {
      let response = resp.encode(resp.SimpleString("PONG"))

      #(response, state)
    }

    command.Echo(value) -> {
      let response = resp.encode(value)

      #(response, state)
    }

    command.Set(key, value, expiry) -> {
      let response = resp.encode(resp.SimpleString("OK"))
      let state =
        state.get_store(state)
        |> store.upsert(key, value, expiry)
        |> state.update_store(state)

      #(response, state)
    }

    command.Get(key) -> {
      let response = case store.get(state.get_store(state), key) {
        Ok(value) -> resp.encode(value)
        Error(_) -> resp.encode(resp.Null)
      }

      #(response, state)
    }

    command.Info(command.Replication) -> {
      let #(role, replid, repl_offset) = case state.get_config(state) {
        state.Slave(_, _) -> #(Some("slave"), None, None)

        state.Master(_, replid, repl_offset) -> #(
          Some("master"),
          Some(replid),
          Some(int.to_string(repl_offset)),
        )
      }

      let response =
        [#("role", role), #("master_replid", replid), #("master_repl_offset", repl_offset)]
        |> list.filter_map(fn(curr) {
          let #(key, value) = curr

          case value {
            Some(value) -> Ok(key <> ":" <> value)
            None -> Error(Nil)
          }
        })
        |> string.join("\r\n")
        |> resp.BulkString()
        |> resp.encode()

      #(response, state)
    }
  }

  let assert Ok(_) = glisten.send(conn, bytes_builder.from_bit_array(response))

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
