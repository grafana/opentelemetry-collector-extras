# OpenTelemetry Collector Extras

A collection of [OpenTelemetry Collector](https://github.com/open-telemetry/opentelemetry-collector) components maintained by [Grafana Labs](https://grafana.com). This repository contains components that are not yet ready or suitable for inclusion in [opentelemetry-collector-contrib](https://github.com/open-telemetry/opentelemetry-collector-contrib).

Components here may be experimental, under active development, or specific to Grafana's use cases. When components mature, they may be proposed for upstream contribution to opentelemetry-collector-contrib.

## Component Types

This repository contains the following OpenTelemetry Collector component types:

- **[Processors](processor/)** - transform, filter, or enrich telemetry data
- **[Extensions](extension/)** - provide additional collector capabilities

## Using These Components

Components from this repository can be included in a custom collector distribution using the [OpenTelemetry Collector Builder (ocb)](https://github.com/open-telemetry/opentelemetry-collector/tree/main/cmd/builder).

## Contributing

Contributions are welcome! Please open an issue to discuss your proposed changes before submitting a pull request.

## License

See [LICENSE](LICENSE).
