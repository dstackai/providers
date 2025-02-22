# gpuhunt

[`dstackai/gpuhunt`](https://github.com/dstackai/gpuhunt) is a library developed and used for dstack. It implements the unified interface for fetching offers and prices from different cloud providers.

An offer is a possible configuration. It consists of:
- Provider (or backend in dstack)
- CPU count
- RAM size
- Disk size
- GPU count
- GPU model name (if any)
- GPU VRAM size (if any)
- Is interruptible (or spot)
- Region (provider-specific)
- Instance name or ID (provider-specific)
- Price per hour

## Catalog

Some providers don't have a suitable API for querying all offers in real-time. That's why gpuhunt has two types of providers:

- Online — offers can be queried in real-time
- Offline — offers must be loaded from a precomputed catalog file

The `Catalog` class hides those details from the user, reading offers from the file for offline providers or querying online providers.

The `Catalog` class pulls the latest catalog from the S3 bucket and caches it for 4 hours in memory.

## Provider implementation

Providers must implement a single method `get`. It has the same name for both online and offline providers but works differently.

### Offers sorting

The provider is responsible for correct offer sorting. Usually, sorting by price works fine, but sometimes the cheapest offer should not be tried first. Later, `Catalog` will merge offers from different providers based on the lowest price.

### QueryFilter

`QueryFilter` wraps options for offer filtering:

- min/max for numerical features
- allowed list for names

`QueryFilter` is passed to online providers. It should be used as a guide for offer search optimization. However, `Catalog` will double-check all returned filters to see if they match the filter.

Offline providers don't get `QueryFilter` because it is not known during catalog collection.

### balance_resources

Some providers offer extreme flexibility in possible configurations, but not all of them are practical. For example, an A100 GPU with 2 GB of RAM doesn't make any sense. `balance_resources` tells the provider implementation to apply heuristics for fields missing in `QueryFilter`. Therefore, `balance_resources` is also only used by online providers.

`dstack` always sets `balance_resources=True` if it is supported.

## Offline providers

### AWS

- Downloads large CSV containing all prices for on-demand instances
- Filters out if: non-Linux, reserved pricing, outdated families, with some software, not supported family
- Queries spot pricing in every known region (in parallel)

### Azure

- Queries all prices for compute resources (in parallel)
- Filters out if: outdated family, not supported family
- Queries configuration details to fill CPU, RAM, and GPU information

### DataCrunch

- Just queries all offers via API

### GCP

- Queries all preconfigured instances
- Adds all possible GPU combinations with `n1` instances
- Adds prices doing heuristic families to SKUs mapping

### Lambda Labs

- Queries all known configurations
- Adds all known regions to all configurations

### OCI

- Parses Oracle's [Cost Estimator](https://www.oracle.com/cloud/costestimator.html) datasets
- Duplicates each offer in all regions, since prices are the same everywhere and availability is mostly the same

### Nebius

- Uses hardcoded CPU and GPU platforms configurations
- Applies SKUs pricing to the platforms

### GitHub Actions: collect catalog

The offline catalog is built in GitHub Actions every night. Every offline provider produces a CSV file with offers. Later, those files get compressed into a zip archive and uploaded to the public S3 bucket.

To ensure data quality, there is a catalog integrity testing step. It uses some simple heuristics to avoid empty catalog files, zero prices, or missing regions.

### Backward compatibility

The same `gpuhunt` version can be used by different `dstack` versions.
Additionally, offline catalogs are produced by the latest `gpuhunt` version, but used by all `dstack` versions.

These mechanisms are used to preserve backward compatibility:

- **`gpuhunt` version**: The interfaces in the `gpuhunt` package preserve backward compatibility
  within a minor version (`X` in `0.X.Y`).
- **Offer flags**: If an offer breaks older `dstack` versions, it is marked with a flag in `RawCatalogItem.flags`
  and the flag is added to the list of supported flags in `dstack`.
  Older `dstack` versions that don't support this flag will not see the respective offers.
- **Offline catalog versions**: If a breaking change in the structure or content of an offline catalog is unavoidable,
  a new version of the catalog can be introduced. Catalog versions are published at `s3://dstack-gpu-pricing/v{N}`.
