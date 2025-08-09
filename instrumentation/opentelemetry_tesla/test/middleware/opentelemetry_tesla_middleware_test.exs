defmodule Tesla.Middleware.OpenTelemetryTest do
  use ExUnit.Case, async: true

  alias OpenTelemetry.SemConv.ErrorAttributes
  alias OpenTelemetry.SemConv.NetworkAttributes
  alias OpenTelemetry.SemConv.ServerAttributes
  alias OpenTelemetry.SemConv.UserAgentAttributes
  alias OpenTelemetry.SemConv.Incubating.HTTPAttributes
  alias OpenTelemetry.SemConv.Incubating.URLAttributes

  require Record

  for {name, spec} <- Record.extract_all(from_lib: "opentelemetry/include/otel_span.hrl") do
    Record.defrecord(name, spec)
  end

  for {name, spec} <- Record.extract_all(from_lib: "opentelemetry_api/include/opentelemetry.hrl") do
    Record.defrecord(name, spec)
  end

  setup do
    bypass = Bypass.open()

    :application.stop(:opentelemetry)
    :application.set_env(:opentelemetry, :tracer, :otel_tracer_default)

    :application.set_env(:opentelemetry, :processors, [
      {:otel_batch_processor, %{scheduled_delay_ms: 1, exporter: {:otel_exporter_pid, self()}}}
    ])

    :application.start(:opentelemetry)

    {:ok, bypass: bypass, base_url: endpoint_url(bypass.port)}
  end

  describe "basic functionality" do
    test "Records spans for Tesla HTTP client", %{bypass: bypass, base_url: base_url} do
      Bypass.expect_once(bypass, "GET", "/users", fn conn ->
        Plug.Conn.resp(conn, 204, "")
      end)

      client =
        Tesla.client([
          {Tesla.Middleware.BaseUrl, base_url},
          Tesla.Middleware.OpenTelemetry
        ])

      Tesla.get(client, "/users/")

      assert_receive {:span, span(name: name, attributes: attributes)}
      assert name == "GET"

      attrs = :otel_attributes.map(attributes)

      expected_attrs = [
        {HTTPAttributes.http_request_method(), :GET},
        {HTTPAttributes.http_response_status_code(), 204},
        {ServerAttributes.server_address(), "localhost"},
        {ServerAttributes.server_port(), bypass.port},
        {URLAttributes.url_full(), "#{base_url}users/"}
      ]

      for {attr, expected} <- expected_attrs do
        actual = Map.get(attrs, attr)
        assert expected == actual, "#{attr} expected #{expected} got #{actual}"
      end
    end

    test "basic request with all opt-ins", %{bypass: bypass, base_url: base_url} do
      Bypass.expect_once(bypass, "POST", "/users/3", fn conn ->
        conn
        |> Plug.Conn.put_status(200)
        |> Plug.Conn.put_resp_header("content-type", "application/json")
        |> Plug.Conn.resp(200, "test response body")
      end)

      client =
        Tesla.client([
          {Tesla.Middleware.BaseUrl, base_url},
          {Tesla.Middleware.OpenTelemetry,
           opt_in_attrs: [
             HTTPAttributes.http_request_body_size(),
             HTTPAttributes.http_response_body_size(),
             NetworkAttributes.network_transport(),
             URLAttributes.url_scheme(),
             URLAttributes.url_template(),
             UserAgentAttributes.user_agent_original()
           ],
           request_header_attrs: ["user-agent"],
           response_header_attrs: ["content-type"]}
        ])

      Tesla.post(client, "/users/3", "test body", headers: [{"user-agent", "test-agent"}])

      assert_receive {:span, span(name: name, attributes: attributes)}
      assert name == "POST"

      attrs = :otel_attributes.map(attributes)

      expected_attrs = [
        {HTTPAttributes.http_request_method(), :POST},
        {HTTPAttributes.http_response_status_code(), 200},
        {HTTPAttributes.http_request_body_size(), 9},
        {HTTPAttributes.http_response_body_size(), 18},
        {NetworkAttributes.network_transport(), :tcp},
        {ServerAttributes.server_address(), "localhost"},
        {ServerAttributes.server_port(), bypass.port},
        {URLAttributes.url_scheme(), :http},
        {String.to_atom("#{HTTPAttributes.http_request_header()}.user-agent"), ["test-agent"]},
        {String.to_atom("#{HTTPAttributes.http_response_header()}.content-type"),
         ["application/json"]}
      ]

      for {attr, expected} <- expected_attrs do
        actual = Map.get(attrs, attr)
        assert expected == actual, "#{attr} expected #{expected} got #{inspect(actual)}"
      end
    end
  end

  describe "span name" do
    test "uses generic route name when opentelemetry middleware is configured before path params middleware",
         %{
           bypass: bypass,
           base_url: base_url
         } do
      Bypass.expect_once(bypass, "GET", "/users/3", fn conn ->
        Plug.Conn.resp(conn, 204, "")
      end)

      client =
        Tesla.client([
          {Tesla.Middleware.BaseUrl, base_url},
          {Tesla.Middleware.OpenTelemetry, opt_in_attrs: [URLAttributes.url_template()]},
          Tesla.Middleware.PathParams
        ])

      Tesla.get(client, "/users/:id", opts: [path_params: [id: "3"]])

      assert_receive {:span, span(name: "GET /users/:id", attributes: _attributes)}
    end

    test "uses low-cardinality method name when path params middleware is not used",
         %{
           bypass: bypass,
           base_url: base_url
         } do
      Bypass.expect_once(bypass, "GET", "/users/", fn conn ->
        Plug.Conn.resp(conn, 204, "")
      end)

      client =
        Tesla.client([
          {Tesla.Middleware.BaseUrl, base_url},
          Tesla.Middleware.OpenTelemetry
        ])

      Tesla.get(client, "/users/")

      assert_receive {:span, span(name: "GET", attributes: _attributes)}
    end

    test "uses low-cardinality method name when url template not enabled",
         %{
           bypass: bypass,
           base_url: base_url
         } do
      Bypass.expect_once(bypass, "GET", "/users/3", fn conn ->
        Plug.Conn.resp(conn, 204, "")
      end)

      client =
        Tesla.client([
          {Tesla.Middleware.BaseUrl, base_url},
          Tesla.Middleware.OpenTelemetry,
          Tesla.Middleware.PathParams
        ])

      Tesla.get(client, "/users/:id", opts: [path_params: [id: "3"]])

      assert_receive {:span, span(name: "GET", attributes: _attributes)}
    end

    test "uses custom span name when passed in middleware opts",
         %{
           bypass: bypass,
           base_url: base_url
         } do
      Bypass.expect_once(bypass, "GET", "/users/3", fn conn ->
        Plug.Conn.resp(conn, 204, "")
      end)

      client =
        Tesla.client([
          {Tesla.Middleware.BaseUrl, base_url},
          {Tesla.Middleware.OpenTelemetry, span_name: "POST :my-high-cardinality-url"},
          Tesla.Middleware.PathParams
        ])

      Tesla.get(client, "/users/:id", opts: [path_params: [id: "3"]])

      assert_receive {:span, span(name: "POST :my-high-cardinality-url", attributes: _attributes)}
    end

    test "uses custom span name function when passed in middleware opts",
         %{
           bypass: bypass,
           base_url: base_url
         } do
      Bypass.expect_once(bypass, "GET", "/users/3", fn conn ->
        Plug.Conn.resp(conn, 204, "")
      end)

      client =
        Tesla.client([
          {Tesla.Middleware.BaseUrl, base_url},
          {Tesla.Middleware.OpenTelemetry,
           span_name: fn env ->
             "#{String.upcase(to_string(env.method))} potato"
           end},
          Tesla.Middleware.PathParams
        ])

      Tesla.get(client, "/users/:id", opts: [path_params: [id: "3"]])

      assert_receive {:span, span(name: "GET potato", attributes: _attributes)}
    end
  end

  describe "error handling" do
    @error_codes [
      400,
      401,
      402,
      403,
      404,
      405,
      406,
      407,
      408,
      409,
      410,
      411,
      412,
      413,
      414,
      415,
      416,
      417,
      418,
      500,
      501,
      502,
      503,
      504,
      505,
      506,
      507,
      508
    ]

    for code <- @error_codes do
      test "Marks Span status as :error when HTTP request fails with #{code}", %{
        bypass: bypass,
        base_url: base_url
      } do
        Bypass.expect_once(bypass, "GET", "/users", fn conn ->
          Plug.Conn.resp(conn, unquote(code), "")
        end)

        client =
          Tesla.client([
            {Tesla.Middleware.BaseUrl, base_url},
            Tesla.Middleware.OpenTelemetry
          ])

        Tesla.get(client, "/users/")

        assert_receive {:span, span(status: {:status, :error, ""}, attributes: attributes)}

        attrs = :otel_attributes.map(attributes)
        assert Map.get(attrs, ErrorAttributes.error_type()) == to_string(unquote(code))
      end
    end

    test "Marks Span status as :error when max redirects are exceeded", %{
      bypass: bypass,
      base_url: base_url
    } do
      Bypass.expect(bypass, "GET", "/users", fn conn ->
        conn
        |> Plug.Conn.put_resp_header("Location", "/users/1")
        |> Plug.Conn.resp(301, "")
      end)

      Bypass.expect(bypass, "GET", "/users/1", fn conn ->
        conn
        |> Plug.Conn.put_resp_header("Location", "/users/2")
        |> Plug.Conn.resp(301, "")
      end)

      client =
        Tesla.client([
          {Tesla.Middleware.BaseUrl, base_url},
          Tesla.Middleware.OpenTelemetry,
          {Tesla.Middleware.FollowRedirects, max_redirects: 1}
        ])

      Tesla.get(client, "/users/")

      assert_receive {:span, span(status: {:status, :error, ""})}
    end

    test "Marks Span status as :ok if error status is within `mark_status_ok` opt list",
         %{bypass: bypass, base_url: base_url} do
      Bypass.expect_once(bypass, "GET", "/users", fn conn ->
        Plug.Conn.resp(conn, 404, "")
      end)

      client =
        Tesla.client([
          {Tesla.Middleware.BaseUrl, base_url},
          {Tesla.Middleware.OpenTelemetry, mark_status_ok: [404]}
        ])

      Tesla.get(client, "/users/")

      assert_receive {:span, span(status: {:status, :ok, ""})}
    end

    test "Marks Span status as :error unless error status is within `mark_status_ok` opt list",
         %{bypass: bypass, base_url: base_url} do
      Bypass.expect_once(bypass, "GET", "/users", fn conn ->
        Plug.Conn.resp(conn, 404, "")
      end)

      client =
        Tesla.client([
          {Tesla.Middleware.BaseUrl, base_url},
          {Tesla.Middleware.OpenTelemetry, mark_status_ok: []}
        ])

      Tesla.get(client, "/users/")

      assert_receive {:span, span(status: {:status, :error, ""}, attributes: attributes)}

      attrs = :otel_attributes.map(attributes)
      assert Map.get(attrs, ErrorAttributes.error_type()) == "404"
    end
  end

  describe "URL handling" do
    test "Appends query string parameters to url.full attribute", %{
      bypass: bypass,
      base_url: base_url
    } do
      Bypass.expect_once(bypass, "GET", "/users/2", fn conn ->
        Plug.Conn.resp(conn, 204, "")
      end)

      client =
        Tesla.client([
          {Tesla.Middleware.BaseUrl, base_url},
          Tesla.Middleware.OpenTelemetry,
          Tesla.Middleware.PathParams,
          {Tesla.Middleware.Query, [token: "some-token", array: ["foo", "bar"]]}
        ])

      Tesla.get(client, "/users/:id", opts: [path_params: [id: "2"]])

      assert_receive {:span, span(name: _name, attributes: attributes)}

      mapped_attributes = :otel_attributes.map(attributes)

      assert mapped_attributes[URLAttributes.url_full()] ==
               "http://localhost:#{bypass.port}/users/2?token=some-token&array%5B%5D=foo&array%5B%5D=bar"
    end

    test "url.full attribute is correct when request doesn't contain query string parameters", %{
      bypass: bypass,
      base_url: base_url
    } do
      Bypass.expect_once(bypass, "GET", "/users/2", fn conn ->
        Plug.Conn.resp(conn, 204, "")
      end)

      client =
        Tesla.client([
          {Tesla.Middleware.BaseUrl, base_url},
          Tesla.Middleware.OpenTelemetry,
          Tesla.Middleware.PathParams,
          {Tesla.Middleware.Query, []}
        ])

      Tesla.get(client, "/users/:id", opts: [path_params: [id: "2"]])

      assert_receive {:span, span(name: _name, attributes: attributes)}

      mapped_attributes = :otel_attributes.map(attributes)

      assert mapped_attributes[URLAttributes.url_full()] ==
               "http://localhost:#{bypass.port}/users/2"
    end
  end

  describe "content length handling" do
    test "Records http.response_body_size param into the span", %{
      bypass: bypass,
      base_url: base_url
    } do
      Bypass.expect_once(bypass, "GET", "/users/2", fn conn ->
        Plug.Conn.resp(conn, 200, "HELLO 👋")
      end)

      client =
        Tesla.client([
          {Tesla.Middleware.BaseUrl, base_url},
          {Tesla.Middleware.OpenTelemetry,
           opt_in_attrs: [HTTPAttributes.http_response_body_size()]},
          Tesla.Middleware.PathParams,
          {Tesla.Middleware.Query, [token: "some-token"]}
        ])

      Tesla.get(client, "/users/:id", opts: [path_params: [id: "2"]])

      assert_receive {:span, span(name: _name, attributes: attributes)}

      mapped_attributes = :otel_attributes.map(attributes)

      {response_size, _} =
        Integer.parse(to_string(mapped_attributes[HTTPAttributes.http_response_body_size()]))

      assert response_size == byte_size("HELLO 👋")
    end
  end

  describe "retry count handling" do
    test "Records http.request.resend_count when retry_count is provided", %{
      bypass: bypass,
      base_url: base_url
    } do
      Bypass.expect_once(bypass, "GET", "/users", fn conn ->
        Plug.Conn.resp(conn, 200, "OK")
      end)

      client =
        Tesla.client([
          {Tesla.Middleware.BaseUrl, base_url},
          {Tesla.Middleware.OpenTelemetry,
           opt_in_attrs: [HTTPAttributes.http_request_resend_count()]}
        ])

      Tesla.get(client, "/users", opts: [retry_count: 2])

      assert_receive {:span, span(name: _name, attributes: attributes)}

      mapped_attributes = :otel_attributes.map(attributes)
      assert mapped_attributes[HTTPAttributes.http_request_resend_count()] == 2
    end

    test "Does not include http.request.resend_count when no retry_count provided", %{
      bypass: bypass,
      base_url: base_url
    } do
      Bypass.expect_once(bypass, "GET", "/users", fn conn ->
        Plug.Conn.resp(conn, 200, "OK")
      end)

      client =
        Tesla.client([
          {Tesla.Middleware.BaseUrl, base_url},
          {Tesla.Middleware.OpenTelemetry,
           opt_in_attrs: [HTTPAttributes.http_request_resend_count()]}
        ])

      Tesla.get(client, "/users")

      assert_receive {:span, span(name: _name, attributes: attributes)}

      mapped_attributes = :otel_attributes.map(attributes)
      refute Map.has_key?(mapped_attributes, HTTPAttributes.http_request_resend_count())
    end
  end

  describe "trace propagation" do
    test "injects distributed tracing headers by default" do
      {:ok, env} = Tesla.get(client(), "/propagate-traces")

      assert traceparent = Tesla.get_header(env, "traceparent")
      assert is_binary(traceparent)

      assert_receive {:span, span(name: _name, attributes: _attributes)}
    end

    test "optionally disable propagation but keep span report" do
      {:ok, env} = Tesla.get(client(propagator: :none), "/propagate-traces")

      refute Tesla.get_header(env, "traceparent")

      assert_receive {:span, span(name: _name, attributes: _attributes)}
    end
  end

  describe "ports" do
    test "when port present" do
      client = Tesla.client([Tesla.Middleware.OpenTelemetry])
      Tesla.get(client, "http://localtest:8080/ok")

      assert_receive {:span, span(attributes: span_attrs)}

      attrs = :otel_attributes.map(span_attrs)
      assert 8080 == Map.get(attrs, ServerAttributes.server_port())
    end

    test "when port not set and http scheme" do
      client = Tesla.client([Tesla.Middleware.OpenTelemetry])
      Tesla.get(client, "http://localtest/ok")

      assert_receive {:span, span(attributes: span_attrs)}

      attrs = :otel_attributes.map(span_attrs)
      assert 80 == Map.get(attrs, ServerAttributes.server_port())
    end

    test "when port not set and https scheme" do
      client = Tesla.client([Tesla.Middleware.OpenTelemetry])
      Tesla.get(client, "https://localtest/ok")

      assert_receive {:span, span(attributes: span_attrs)}

      attrs = :otel_attributes.map(span_attrs)
      assert 443 == Map.get(attrs, ServerAttributes.server_port())
    end
  end

  defp client(opts \\ []) do
    [{Tesla.Middleware.OpenTelemetry, opts}]
    |> Tesla.client(fn env -> {:ok, env} end)
  end

  defp endpoint_url(port), do: "http://localhost:#{port}/"
end
