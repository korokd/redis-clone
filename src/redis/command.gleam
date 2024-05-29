import gleam/int
import gleam/option.{type Option, None, Some}
import gleam/string

import redis/resp

pub type InfoSection {
  Replication
}

pub type Command {
  Ping
  Echo(value: String)
  Set(key: String, value: resp.RespData, expiry: Option(Int))
  Get(key: String)
  Info(InfoSection)
}

pub type CommandError {
  UnexpectedArguments(command: String, arguments: List(resp.RespData))
  UnknownCommand(command: String)
}

pub fn from_resp_data(data: resp.Parsed) -> Result(Command, CommandError) {
  let resp.Parsed(resp_data, _) = data

  case resp_data {
    resp.Array(elements) -> {
      let assert [resp.String(command), ..arguments] = elements

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

        "info", [resp.String(replication)] -> case string.lowercase(replication) {
          "replication" -> Ok(Info(Replication))

          _ -> Error(UnexpectedArguments(command, arguments))
        }

        "ping", _ -> Error(UnexpectedArguments(command, arguments))
        "echo", _ -> Error(UnexpectedArguments(command, arguments))
        "set", _ -> Error(UnexpectedArguments(command, arguments))
        "get", _ -> Error(UnexpectedArguments(command, arguments))

        _, _ -> Error(UnknownCommand(command))
      }
    }

    resp.String(command) -> {
      case command {
        "ping" -> Ok(Ping)

        _ -> Error(UnknownCommand(command))
      }
    }

    resp.Null -> {
      panic as "Unexpected Null"
    }
  }
}
