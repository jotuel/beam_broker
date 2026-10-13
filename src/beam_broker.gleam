import beam_broker/router
import beam_broker/supervisor
import gleam/erlang/process
import logging
import mist
import wisp
import wisp/wisp_mist

pub fn main() -> Nil {
  logging.configure()
  wisp.configure_logger()
  logging.log(logging.Info, "Starting beam_broker")
  case supervisor.start_orchestrator() {
    Ok(_) -> {
      let assert Ok(_) = start_web_server()
      process.sleep_forever()
    }
    Error(_) -> logging.log(logging.Error, "Failed to start orchestrator")
  }
}

fn start_web_server() {
  wisp_mist.handler(router.handle_request, wisp.random_string(64))
  |> mist.new
  |> mist.port(8000)
  |> mist.start
}
