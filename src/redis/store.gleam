import gleam/dict.{type Dict}
import gleam/option.{type Option, None, Some}

import birl

import redis/resp

pub opaque type StoreError {
  Expired
  NotFound
}

type Key =
  String

type ValueRaw =
  resp.RespData

type Expiry =
  Option(Int)

pub opaque type Metadata {
  Metadata(created_at: Int, updated_at: Int, expiry: Option(Int))
}

type Value =
  #(ValueRaw, Metadata)

pub type Store =
  Dict(Key, Value)

pub fn new() -> Store {
  dict.new()
}

pub fn upsert(store: Store, key: Key, value: ValueRaw, expiry: Expiry) -> Store {
  let updated_at =
    birl.now()
    |> birl.to_unix_milli()

  let created_at = case dict.get(store, key) {
    Ok(#(_, Metadata(created_at, _, _))) -> created_at
    Error(_) -> updated_at
  }

  let metadata = Metadata(created_at, updated_at, expiry)

  dict.insert(store, key, #(value, metadata))
}

pub fn get(store: Store, key: Key) -> Result(ValueRaw, StoreError) {
  case dict.get(store, key) {
    Ok(#(value, Metadata(_, updated_at, expiry))) -> {
      let now =
        birl.now()
        |> birl.to_unix_milli()

      case expiry {
        Some(expiry) ->
          case now > updated_at + expiry {
            True -> Error(Expired)
            False -> Ok(value)
          }

        None -> Ok(value)
      }
    }

    Error(_) -> Error(NotFound)
  }
}
