use std::sync::Arc;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use zarrs::array::codec::array_to_bytes::sharding::ShardingCodecBuilder;

fn array_write_all(c: &mut Criterion) {
    let mut group = c.benchmark_group("array_write_all");
    for size in [128u64, 256u64, 512u64].iter() {
        let num_elements: u64 = size * size * size;
        group.throughput(Throughput::Bytes(num_elements));
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            b.iter(|| {
                let store = zarrs::storage::store::MemoryStore::new();
                let array = zarrs::array::ArrayBuilder::new(
                    vec![size; 3],
                    zarrs::array::DataType::UInt8,
                    vec![32; 3].try_into().unwrap(),
                    zarrs::array::FillValue::from(0u8),
                )
                .build(store.into(), "/")
                .unwrap();
                let data = vec![1u8; num_elements.try_into().unwrap()];
                let subset = zarrs::array_subset::ArraySubset::new_with_shape(vec![size; 3]);
                array.store_array_subset_elements(&subset, &data).unwrap();
            });
        });
    }
    group.finish();
}

fn array_write_all_sharded(c: &mut Criterion) {
    let mut group = c.benchmark_group("array_write_all_sharded");
    for size in [128u64, 256u64, 512u64].iter() {
        let num_elements: u64 = size * size * size;
        group.throughput(Throughput::Bytes(num_elements));
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            b.iter(|| {
                let store = zarrs::storage::store::MemoryStore::new();
                let sharding_codec =
                    Arc::new(ShardingCodecBuilder::new(vec![32; 3].try_into().unwrap()).build());
                let array = zarrs::array::ArrayBuilder::new(
                    vec![size; 3],
                    zarrs::array::DataType::UInt16,
                    vec![size; 3].try_into().unwrap(),
                    zarrs::array::FillValue::from(0u16),
                )
                .array_to_bytes_codec(sharding_codec)
                .build(store.into(), "/")
                .unwrap();
                let data = vec![1u16; num_elements.try_into().unwrap()];
                let subset = zarrs::array_subset::ArraySubset::new_with_shape(vec![size; 3]);
                array.store_array_subset_elements(&subset, &data).unwrap();
            });
        });
    }
    group.finish();
}

fn array_read_all(c: &mut Criterion) {
    let mut group = c.benchmark_group("array_read_all");
    for size in [128u64, 256u64, 512u64].iter() {
        let num_elements: u64 = size * size * size;
        group.throughput(Throughput::Bytes(num_elements));
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            // Write the data
            let store = zarrs::storage::store::MemoryStore::new();
            let array = zarrs::array::ArrayBuilder::new(
                vec![size; 3],
                zarrs::array::DataType::UInt16,
                vec![32; 3].try_into().unwrap(),
                zarrs::array::FillValue::from(0u16),
            )
            .build(store.into(), "/")
            .unwrap();
            let data = vec![1u16; num_elements.try_into().unwrap()];
            let subset = zarrs::array_subset::ArraySubset::new_with_shape(vec![size; 3]);
            array.store_array_subset_elements(&subset, &data).unwrap();

            // Benchmark reading the data
            b.iter(|| {
                let _bytes = array.retrieve_array_subset(&subset).unwrap();
            });
        });
    }
    group.finish();
}

fn array_read_all_sharded(c: &mut Criterion) {
    let mut group = c.benchmark_group("array_read_all_sharded");
    for size in [128u64, 256u64, 512u64].iter() {
        let num_elements: u64 = size * size * size;
        group.throughput(Throughput::Bytes(num_elements));
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            // Write the data
            let store = zarrs::storage::store::MemoryStore::new();
            let sharding_codec =
                Arc::new(ShardingCodecBuilder::new(vec![32; 3].try_into().unwrap()).build());
            let array = zarrs::array::ArrayBuilder::new(
                vec![size; 3],
                zarrs::array::DataType::UInt8,
                vec![size; 3].try_into().unwrap(),
                zarrs::array::FillValue::from(1u8),
            )
            .array_to_bytes_codec(sharding_codec)
            .build(store.into(), "/")
            .unwrap();
            let data = vec![0u8; num_elements.try_into().unwrap()];
            let subset = zarrs::array_subset::ArraySubset::new_with_shape(vec![size; 3]);
            array.store_array_subset_elements(&subset, &data).unwrap();

            // Benchmark reading the data
            b.iter(|| {
                let _bytes = array.retrieve_array_subset(&subset).unwrap();
            });
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    array_write_all,
    array_read_all,
    array_write_all_sharded,
    array_read_all_sharded
);
criterion_main!(benches);
