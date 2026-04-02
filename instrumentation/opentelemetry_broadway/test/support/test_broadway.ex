defmodule TestBroadway do
  use Broadway

  require OpenTelemetry.Tracer

  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, __MODULE__)
    test_pid = Keyword.get(opts, :test_pid)

    Broadway.start_link(__MODULE__,
      name: name,
      context: %{test_pid: test_pid},
      producer: [
        module: {Broadway.DummyProducer, []}
      ],
      processors: [
        default: []
      ],
      batchers: [
        default: []
      ]
    )
  end

  @impl true
  def handle_message(_processor, %Broadway.Message{} = message, %{test_pid: test_pid}) do
    case message.data do
      "success" ->
        message

      "error" ->
        if test_pid, do: send(test_pid, {:handle_message_span_ctx, OpenTelemetry.Tracer.current_span_ctx()})
        Broadway.Message.failed(message, "something went wrong")

      "exception" ->
        raise RuntimeError, "an exception occurred"
    end
  end

  @impl true
  def handle_batch(_batcher, messages, _batch_info, _context) do
    messages
  end

  @impl true
  def handle_failed(messages, %{test_pid: test_pid}) do
    if test_pid, do: send(test_pid, {:handle_failed_span_ctx, OpenTelemetry.Tracer.current_span_ctx()})
    messages
  end
end
