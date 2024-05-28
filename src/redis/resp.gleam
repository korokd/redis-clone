import gleam/bit_array
import gleam/int
import gleam/list
import gleam/result

pub type RespData {
  Array(elements: List(RespData))
  String(content: String)
  Null
}

pub type Parsed {
  Parsed(resp_data: RespData, remaining_input: BitArray)
}

pub type ParseError {
  InvalidUnicode
  NotEnoughInput
  UnexpectedInput(BitArray)
}

pub fn encode(input: RespData) -> BitArray {
  en(<<>>, input)
}

fn en(acc: BitArray, data: RespData) -> BitArray {
  case data {
    String(content) -> {
      let bytes = int.to_string(bit_array.byte_size(<<content:utf8>>))
      <<acc:bits, "$":utf8, bytes:utf8, "\r\n":utf8, content:utf8, "\r\n":utf8>>
    }

    Array(elements) -> {
      let length = int.to_string(list.length(elements))
      let acc = <<acc:bits, "*":utf8, length:utf8, "\r\n":utf8>>
      list.fold(elements, acc, en)
    }

    Null -> <<acc:bits, "$-1\r\n":utf8>>
  }
}

pub fn parse(input: BitArray) -> Result(Parsed, ParseError) {
  case input {
    // The three ways of representing null
    <<"-\r\n":utf8, rest:bits>> -> Ok(Parsed(Null, rest))
    <<"$-1\r\n":utf8, rest:bits>> -> Ok(Parsed(Null, rest))
    <<"*-1\r\n":utf8, rest:bits>> -> Ok(Parsed(Null, rest))

    <<"+":utf8, rest:bits>> -> parse_simple_string(rest, <<>>)
    <<"$":utf8, rest:bits>> -> parse_bulk_string(rest)
    <<"*":utf8, rest:bits>> -> parse_array(rest)

    input -> Error(UnexpectedInput(input))
  }
}

fn parse_simple_string(
  input: BitArray,
  acc: BitArray,
) -> Result(Parsed, ParseError) {
  case input {
    <<>> -> Error(NotEnoughInput)

    <<"\r\n":utf8, input:bits>> -> {
      use content <- result.try(parse_unicode(acc))
      Ok(Parsed(String(content), input))
    }

    <<c, input:bits>> -> parse_simple_string(input, <<acc:bits, c>>)

    input -> Error(UnexpectedInput(input))
  }
}

fn parse_bulk_string(input: BitArray) -> Result(Parsed, ParseError) {
  use #(length, input) <- result.try(parse_raw_int(input, 0))

  let total_length = bit_array.byte_size(input)
  let content = bit_array.slice(input, 0, length)
  let rest = bit_array.slice(input, length, total_length - length)

  case content, rest {
    _, Ok(<<>>) -> Error(NotEnoughInput)

    _, Ok(<<"\r":utf8>>) -> Error(NotEnoughInput)

    Ok(content), Ok(<<"\r\n":utf8, rest:bits>>) -> {
      use content <- result.try(parse_unicode(content))
      Ok(Parsed(String(content), rest))
    }

    _, Ok(rest) -> Error(UnexpectedInput(rest))

    _, _ -> Error(UnexpectedInput(input))
  }
}

fn parse_raw_int(
  input: BitArray,
  acc: Int,
) -> Result(#(Int, BitArray), ParseError) {
  case input {
    <<"0":utf8, input:bits>> -> parse_raw_int(input, acc * 10)
    <<"1":utf8, input:bits>> -> parse_raw_int(input, 1 + acc * 10)
    <<"2":utf8, input:bits>> -> parse_raw_int(input, 2 + acc * 10)
    <<"3":utf8, input:bits>> -> parse_raw_int(input, 3 + acc * 10)
    <<"4":utf8, input:bits>> -> parse_raw_int(input, 4 + acc * 10)
    <<"5":utf8, input:bits>> -> parse_raw_int(input, 5 + acc * 10)
    <<"6":utf8, input:bits>> -> parse_raw_int(input, 6 + acc * 10)
    <<"7":utf8, input:bits>> -> parse_raw_int(input, 7 + acc * 10)
    <<"8":utf8, input:bits>> -> parse_raw_int(input, 8 + acc * 10)
    <<"9":utf8, input:bits>> -> parse_raw_int(input, 9 + acc * 10)

    <<"\r\n":utf8, input:bits>> -> Ok(#(acc, input))

    <<"\r":utf8>> | <<>> -> Error(NotEnoughInput)

    _ -> Error(UnexpectedInput(input))
  }
}

fn parse_unicode(input: BitArray) -> Result(String, ParseError) {
  case bit_array.to_string(input) {
    Ok(content) -> Ok(content)

    Error(_) -> Error(InvalidUnicode)
  }
}

fn parse_array(input: BitArray) -> Result(Parsed, ParseError) {
  case parse_raw_int(input, 0) {
    Error(e) -> Error(e)

    Ok(#(count, input)) ->
      case parse_elements(input, [], count) {
        Error(e) -> Error(e)

        Ok(#(elements, input)) -> Ok(Parsed(Array(elements), input))
      }
  }
}

fn parse_elements(
  input: BitArray,
  acc: List(RespData),
  remaining: Int,
) -> Result(#(List(RespData), BitArray), ParseError) {
  case remaining <= 0 {
    True -> Ok(#(list.reverse(acc), input))

    False -> {
      case parse(input) {
        Ok(parsed) -> {
          let acc = [parsed.resp_data, ..acc]
          parse_elements(parsed.remaining_input, acc, remaining - 1)
        }

        Error(e) -> Error(e)
      }
    }
  }
}
