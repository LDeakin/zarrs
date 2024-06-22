## Correctness Issues with Past Versions
- Prior to zarrs [v0.11.5](https://github.com/LDeakin/zarrs/releases/tag/v0.11.5), arrays that used the `crc32c` codec have invalid chunk checksums
  - These arrays will fail to be read by Zarr implementations if they validate checksums
  - These arrays can be read by zarrs if the [validate checksums](crate::config::Config#validate-checksums) global configuration option is disabled or the relevant codec option is set explicitly
- From zarrs [v0.11.2](https://github.com/LDeakin/zarrs/releases/tag/v0.11.2)-[v0.11.3](https://github.com/LDeakin/zarrs/releases/tag/v0.11.3), the codec configuration of the `crc32c` codec or `bytes` codec (with unspecified endianness) does not conform to the Zarr specification
  - These arrays will fail to be read by other Zarr implementations
  - zarrs still supports reading these arrays, but this may become an error in a future release
  - Fixing these arrays only requires a simple metadata correction, e.g.
    - `sed -i -E "s/(^([ tab]+)\"(crc32c|bytes)\"(,?)$)/\2{ \"name\": \"\3\" }\4/" zarr.json`

## Fixing Erroneous Arrays
[zarrs_tools](https://github.com/LDeakin/zarrs_tools) v0.2.3+ can fix arrays with the above correctness issues with `zarrs_reencode`. Example:
```bash
zarrs_reencode --ignore-checksums array.zarr array_fixed.zarr
```
