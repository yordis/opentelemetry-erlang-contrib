defmodule Tesla.Middleware.OpenTelemetry do
  @moduledoc """
  Creates OpenTelemetry spans and injects tracing headers into HTTP requests

  When used with `Tesla.Middleware.PathParams`, the span name will be created
  based on the provided path. Without it, the span name follow OpenTelemetry
  standards and use just the method name, if not being overridden by opts.

  NOTE: This middleware needs to come before `Tesla.Middleware.PathParams`

  ## Semantic Conventions

  All available required and recommended [Client HTTP Span](https://opentelemetry.io/docs/specs/semconv/http/http-spans/#http-client) semantic conventions are implemented.
  Supported opt-in and experimental attributes can be configured using the `opt_in_attrs` option.

  ## Options

    - `:span_name` - override span name. Can be a `String` for a static span name,
    or a function that takes the `Tesla.Env` and returns a `String`
    - `:propagator` - configures trace headers propagators. Setting it to `:none` disables propagation.
    Any module that implements `:otel_propagator_text_map` can be used.
    Defaults to calling `:otel_propagator_text_map.get_text_map_injector/0`
    - `:mark_status_ok` - configures spans with a list of expected HTTP error codes to be marked as `ok`,
    not as an error-containing spans
    - `:opt_in_attrs` - list of opt-in and experimental attributes to include
    - `:request_header_attrs` - list of request headers to add as attributes (lowercase)
    - `:response_header_attrs` - list of response headers to add as attributes (lowercase)

  ### Opt-in Semantic Convention Attributes

  Otel SemConv requires users to explicitly opt in for any attribute with a
  requirement level of `opt-in` or `experimental`. To ensure compatibility, always use the
  SemConv attribute.

  Available opt-in attributes:
  - `HTTPAttributes.http_request_body_size()`
  - `HTTPAttributes.http_response_body_size()`
  - `HTTPAttributes.http_request_resend_count()`
  - `NetworkAttributes.network_transport()`
  - `URLAttributes.url_scheme()`
  - `URLAttributes.url_template()`
  - `UserAgentAttributes.user_agent_original()`

  ### Request and Response Header Attributes

  Request and response header attributes are opt-in and can be set with the
  `request_header_attrs` and `response_header_attrs` options. Values should be lower-case.

  Example:
  ```
  client = Tesla.client([
    {Tesla.Middleware.OpenTelemetry,
     opt_in_attrs: [HTTPAttributes.http_request_body_size()],
     request_header_attrs: ["user-agent", "authorization"],
     response_header_attrs: ["content-type"]}
  ])
  ```
  """

  alias OpenTelemetry.SemConv.ErrorAttributes
  alias OpenTelemetry.SemConv.NetworkAttributes
  alias OpenTelemetry.SemConv.ServerAttributes
  alias OpenTelemetry.SemConv.UserAgentAttributes
  alias OpenTelemetry.SemConv.Incubating.HTTPAttributes
  alias OpenTelemetry.SemConv.Incubating.URLAttributes

  require OpenTelemetry.Tracer

  @behaviour Tesla.Middleware

  opt_ins = [
    HTTPAttributes.http_request_body_size(),
    HTTPAttributes.http_response_body_size(),
    HTTPAttributes.http_request_resend_count(),
    NetworkAttributes.network_transport(),
    URLAttributes.url_scheme(),
    URLAttributes.url_template(),
    UserAgentAttributes.user_agent_original()
  ]

  @options_schema NimbleOptions.new!(
                    opt_in_attrs: [
                      type: {:list, {:in, opt_ins}},
                      default: [],
                      type_spec: quote(do: opt_in_attrs()),
                      doc: """
                      Opt-in and experimental attributes. Use semantic conventions library to ensure compatibility, e.g. `[HTTPAttributes.http_request_body_size()]`

                      #{Enum.map_join(opt_ins, "\n\n", &"  * `#{inspect(&1)}`")}
                      """
                    ],
                    propagator: [
                      type: {:or, [:atom, {:in, [:none]}]},
                      default: nil,
                      doc: "Trace headers propagator. Set to :none to disable propagation."
                    ],
                    request_header_attrs: [
                      type: {:list, :string},
                      default: [],
                      doc: "List of request headers to add as attributes. (lowercase)"
                    ],
                    response_header_attrs: [
                      type: {:list, :string},
                      default: [],
                      doc: "List of response headers to add as attributes. (lowercase)"
                    ],
                    span_name: [
                      type: {:or, [nil, :string, {:fun, 1}]},
                      default: nil,
                      doc:
                        "User defined span name override. Can be a string or a function that takes Tesla.Env and returns a string."
                    ],
                    mark_status_ok: [
                      type: {:list, :integer},
                      default: [],
                      doc: "List of HTTP status codes to mark as :ok instead of :error"
                    ]
                  )

  @typedoc "Use semantic conventions library to ensure compatibility, e.g. `HTTPAttributes.http_request_body_size()`"
  @type opt_in_attr() ::
          unquote(HTTPAttributes.http_request_body_size())
          | unquote(HTTPAttributes.http_response_body_size())
          | unquote(HTTPAttributes.http_request_resend_count())
          | unquote(NetworkAttributes.network_transport())
          | unquote(URLAttributes.url_scheme())
          | unquote(URLAttributes.url_template())
          | unquote(UserAgentAttributes.user_agent_original())

  @type opt_in_attrs() :: [opt_in_attr()]

  @type options() :: [unquote(NimbleOptions.option_typespec(@options_schema))]

  def call(env, next, opts) do
    config =
      opts
      |> NimbleOptions.validate!(@options_schema)
      |> Enum.into(%{})
      |> then(fn config ->
        if Enum.member?(config.opt_in_attrs, URLAttributes.url_template()) do
          Map.put(config, :url_template_enabled, true)
        else
          Map.put(config, :url_template_enabled, false)
        end
      end)

    span_name = get_span_name(env, config)

    OpenTelemetry.Tracer.with_span span_name, %{kind: :client} do
      attrs = build_attrs(env, config)
      OpenTelemetry.Tracer.set_attributes(attrs)

      env
      |> maybe_put_additional_ok_statuses(config[:mark_status_ok])
      |> maybe_propagate(get_propagator(config))
      |> Tesla.run(next)
      |> set_span_attributes(config)
      |> handle_result(config)
    end
  end

  defp get_span_name(_env, %{span_name: span_name}) when is_binary(span_name) do
    span_name
  end

  defp get_span_name(env, %{span_name: span_name_fun}) when is_function(span_name_fun, 1) do
    span_name_fun.(env)
  end

  defp get_span_name(env, config) do
    method = String.upcase(to_string(env.method))

    if config.url_template_enabled do
      case env.opts[:path_params] do
        nil -> method
        _ -> "#{method} #{URI.parse(env.url).path}"
      end
    else
      method
    end
  end

  defp get_propagator(config) do
    case Map.get(config, :propagator) do
      nil -> :opentelemetry.get_text_map_injector()
      :none -> :none
      propagator -> propagator
    end
  end

  defp maybe_propagate(env, :none), do: env

  defp maybe_propagate(env, propagator) do
    :otel_propagator_text_map.inject(
      propagator,
      env,
      fn key, value, env -> Tesla.put_header(env, key, value) end
    )
  end

  defp maybe_put_additional_ok_statuses(env, [_ | _] = additional_ok_statuses) do
    case env.opts[:additional_ok_statuses] do
      nil -> Tesla.put_opt(env, :additional_ok_statuses, additional_ok_statuses)
      _ -> env
    end
  end

  defp maybe_put_additional_ok_statuses(env, _additional_ok_statuses), do: env

  defp set_span_attributes({_, %Tesla.Env{} = env} = result, config) do
    opt_in_attrs = build_response_opt_in_attrs(env, config)
    response_attrs = build_response_attrs(env, config)

    attrs =
      opt_in_attrs
      |> Map.take(config.opt_in_attrs)
      |> Map.merge(response_attrs)

    OpenTelemetry.Tracer.set_attributes(attrs)
    result
  end

  defp set_span_attributes(result, _config) do
    result
  end

  defp handle_result({:ok, %Tesla.Env{status: status, opts: opts} = _env} = result, _config)
       when status >= 400 do
    span_status =
      if status in Keyword.get(opts, :additional_ok_statuses, []), do: :ok, else: :error

    OpenTelemetry.Tracer.set_status(OpenTelemetry.status(span_status, ""))

    if span_status == :error do
      OpenTelemetry.Tracer.set_attributes(%{
        ErrorAttributes.error_type() => to_string(status)
      })
    end

    result
  end

  defp handle_result(
         {:error, {Tesla.Middleware.FollowRedirects, :too_many_redirects}} = result,
         _config
       ) do
    OpenTelemetry.Tracer.set_status(OpenTelemetry.status(:error, ""))

    OpenTelemetry.Tracer.set_attributes(%{
      ErrorAttributes.error_type() => "too_many_redirects"
    })

    result
  end

  defp handle_result({:ok, _env} = result, _config) do
    result
  end

  defp handle_result({:error, reason} = result, _config) do
    OpenTelemetry.Tracer.set_status(OpenTelemetry.status(:error, format_error(reason)))

    OpenTelemetry.Tracer.set_attributes(%{
      ErrorAttributes.error_type() => extract_error_type(reason)
    })

    result
  end

  defp handle_result(result, _config) do
    OpenTelemetry.Tracer.set_status(OpenTelemetry.status(:error, ""))
    result
  end

  defp build_attrs(env, config) do
    # Note: At request time, we can't build the final URL with path params yet
    # because path params are applied later in the middleware chain
    url = env.url
    uri = URI.parse(url)

    opt_in_attrs = build_request_opt_in_attrs(env, config)

    base_attrs = %{
      HTTPAttributes.http_request_method() => parse_method(env.method),
      ServerAttributes.server_address() => uri.host,
      ServerAttributes.server_port() => extract_port(uri),
      URLAttributes.url_full() => url
    }

    request_header_attrs = set_req_header_attrs(env, config)

    opt_in_attrs
    |> Map.take(config.opt_in_attrs)
    |> Map.merge(base_attrs)
    |> Map.merge(request_header_attrs)
  end

  defp sanitize_url(uri) do
    %{uri | userinfo: nil}
    |> URI.to_string()
  end

  defp extract_port(%{port: port}) when is_integer(port), do: port

  defp extract_port(%{scheme: scheme}) do
    case scheme do
      nil -> 80
      "http" -> 80
      "https" -> 443
      _ -> 80
    end
  end

  defp build_request_opt_in_attrs(env, _config) do
    base_attrs = %{
      HTTPAttributes.http_request_body_size() => extract_request_body_size(env),
      NetworkAttributes.network_transport() => :tcp,
      URLAttributes.url_scheme() => extract_scheme(env),
      URLAttributes.url_template() => extract_url_template(env),
      UserAgentAttributes.user_agent_original() => extract_user_agent(env)
    }

    case extract_retry_count(env) do
      count when count > 0 ->
        Map.put(base_attrs, HTTPAttributes.http_request_resend_count(), count)

      _ ->
        base_attrs
    end
  end

  defp build_response_opt_in_attrs(env, _config) do
    %{
      HTTPAttributes.http_response_body_size() => extract_response_body_size(env)
    }
  end

  defp build_response_attrs(env, config) do
    base_attrs = %{
      HTTPAttributes.http_response_status_code() => env.status
    }

    response_header_attrs = set_resp_header_attrs(env, config)

    # Update URL with final resolved path params and query params
    final_url = Tesla.build_url(env.url, env.query)
    url_attrs = %{URLAttributes.url_full() => sanitize_url(URI.parse(final_url))}

    base_attrs
    |> Map.merge(response_header_attrs)
    |> Map.merge(url_attrs)
  end

  defp extract_scheme(env) do
    uri = URI.parse(env.url)

    case uri.scheme do
      nil -> :http
      "http" -> :http
      "https" -> :https
      _ -> :http
    end
  end

  defp extract_url_template(env) do
    case env.opts[:path_params] do
      nil -> ""
      _ -> URI.parse(env.url).path
    end
  end

  defp extract_user_agent(env) do
    case Enum.find(env.headers, fn {k, _v} -> String.downcase(k) == "user-agent" end) do
      nil -> ""
      {_key, user_agent} -> user_agent
    end
  end

  defp extract_retry_count(env) do
    # Tesla doesn't have built-in retry tracking like Req, but we can check for custom retry count
    # This would be set by Tesla.Middleware.Retry or similar middleware
    case Keyword.get(env.opts, :retry_count, 0) do
      count when count > 0 -> count
      _ -> 0
    end
  end

  defp extract_request_body_size(env) do
    case Enum.find(env.headers, fn {k, _v} -> String.downcase(k) == "content-length" end) do
      nil ->
        # If no content-length header, calculate from body if available
        case env.body do
          nil -> 0
          body when is_binary(body) -> byte_size(body)
          _ -> 0
        end

      {_key, content_length} when is_binary(content_length) ->
        String.to_integer(content_length)

      {_key, content_length} when is_integer(content_length) ->
        content_length
    end
  end

  defp extract_response_body_size(env) do
    case Enum.find(env.headers, fn {k, _v} -> String.downcase(k) == "content-length" end) do
      nil ->
        # If no content-length header, calculate from body if available
        case env.body do
          nil -> 0
          body when is_binary(body) -> byte_size(body)
          _ -> 0
        end

      {_key, content_length} when is_binary(content_length) ->
        String.to_integer(content_length)

      {_key, content_length} when is_integer(content_length) ->
        content_length
    end
  end

  defp parse_method(method) do
    case method do
      :connect -> HTTPAttributes.http_request_method_values().connect
      :delete -> HTTPAttributes.http_request_method_values().delete
      :get -> HTTPAttributes.http_request_method_values().get
      :head -> HTTPAttributes.http_request_method_values().head
      :options -> HTTPAttributes.http_request_method_values().options
      :patch -> HTTPAttributes.http_request_method_values().patch
      :post -> HTTPAttributes.http_request_method_values().post
      :put -> HTTPAttributes.http_request_method_values().put
      :trace -> HTTPAttributes.http_request_method_values().trace
    end
  end

  defp set_req_header_attrs(env, config) do
    :otel_http.extract_headers_attributes(
      :request,
      env.headers,
      Map.get(config, :request_header_attrs, [])
    )
  end

  defp set_resp_header_attrs(env, config) do
    :otel_http.extract_headers_attributes(
      :response,
      env.headers,
      Map.get(config, :response_header_attrs, [])
    )
  end

  defp format_error({Tesla.Middleware.FollowRedirects, :too_many_redirects}),
    do: "too_many_redirects"

  defp format_error({Tesla.Middleware.Timeout, :timeout}), do: "request_timeout"
  defp format_error({Tesla.Middleware.Retry, reason}), do: "retry_failed: #{reason}"
  defp format_error(%{__exception__: true} = exception), do: Exception.message(exception)
  defp format_error(:timeout), do: "timeout"
  defp format_error(:nxdomain), do: "nxdomain" 
  defp format_error(:econnrefused), do: "connection_refused"
  defp format_error(:closed), do: "connection_closed"
  defp format_error(reason) when is_atom(reason), do: to_string(reason)
  defp format_error(_), do: ""

  defp extract_error_type({Tesla.Middleware.FollowRedirects, :too_many_redirects}),
    do: "too_many_redirects"

  defp extract_error_type({Tesla.Middleware.Timeout, :timeout}), do: "request_timeout"
  defp extract_error_type({Tesla.Middleware.Retry, _reason}), do: "retry_failed"
  defp extract_error_type(%{__exception__: true} = exception), do: exception.__struct__
  defp extract_error_type(:timeout), do: "timeout"
  defp extract_error_type(:nxdomain), do: "dns_error"
  defp extract_error_type(:econnrefused), do: "connection_refused"
  defp extract_error_type(:closed), do: "connection_closed"
  defp extract_error_type(reason) when is_atom(reason), do: to_string(reason)
  defp extract_error_type(_), do: "unknown"
end
