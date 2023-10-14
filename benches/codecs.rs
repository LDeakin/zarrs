use criterion::{
    criterion_group, criterion_main, AxisScale, BenchmarkId, Criterion, PlotConfiguration,
    Throughput,
};
use zarrs::array::{
    codec::{array_to_bytes::bytes::Endianness, ArrayCodecTraits, BytesCodec},
    ArrayRepresentation, DataType,
};

fn codec_bytes(c: &mut Criterion) {
    let plot_config = PlotConfiguration::default().summary_scale(AxisScale::Logarithmic);
    let mut group = c.benchmark_group("codec_bytes");
    group.plot_config(plot_config);

    // Set the endianness to be the opposite of the target endianness, so the codec does work
    #[cfg(target_endian = "big")]
    let codec = BytesCodec::new(Some(Endianness::Little));
    #[cfg(target_endian = "little")]
    let codec = BytesCodec::new(Some(Endianness::Big));

    for size in [32, 64, 128, 256, 512].iter() {
        let size3 = size * size * size;
        let num_elements = size3 / 2;
        let rep =
            ArrayRepresentation::new(vec![num_elements; 1], DataType::UInt16, 0u16.into()).unwrap();

        let data = vec![0u8; (size3).try_into().unwrap()];
        group.throughput(Throughput::Bytes(size3));
        group.bench_function(BenchmarkId::new("encode", size3), |b| {
            b.iter(|| codec.encode(data.clone(), &rep).unwrap());
        });
        group.bench_function(BenchmarkId::new("decode", size3), |b| {
            b.iter(|| codec.decode(data.clone(), &rep).unwrap());
        });
        group.bench_function(BenchmarkId::new("par_encode", size3), |b| {
            b.iter(|| codec.par_encode(data.clone(), &rep).unwrap());
        });
        group.bench_function(BenchmarkId::new("par_decode", size3), |b| {
            b.iter(|| codec.par_decode(data.clone(), &rep).unwrap());
        });
    }
}

criterion_group!(benches, codec_bytes);
criterion_main!(benches);
