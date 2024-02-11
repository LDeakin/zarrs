use criterion::{
    criterion_group, criterion_main, AxisScale, BenchmarkId, Criterion, PlotConfiguration,
    Throughput,
};
use zarrs::array::{
    codec::{
        array_to_bytes::bytes::Endianness,
        bytes_to_bytes::blosc::{BloscCompressor, BloscShuffleMode},
        ArrayCodecTraits, BloscCodec, BytesCodec, BytesToBytesCodecTraits,
    },
    BytesRepresentation, ChunkRepresentation, DataType,
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
        let rep = ChunkRepresentation::new(
            vec![num_elements.try_into().unwrap(); 1],
            DataType::UInt16,
            0u16.into(),
        )
        .unwrap();

        let data = vec![0u8; size3.try_into().unwrap()];
        group.throughput(Throughput::Bytes(size3));
        // encode and decode have the same implementation
        group.bench_function(BenchmarkId::new("encode_decode", size3), |b| {
            b.iter(|| codec.encode(data.clone(), &rep).unwrap());
        });
    }
}

fn codec_blosc(c: &mut Criterion) {
    let plot_config = PlotConfiguration::default().summary_scale(AxisScale::Logarithmic);
    let mut group = c.benchmark_group("codec_blosc");
    group.plot_config(plot_config);

    let codec = BloscCodec::new(
        BloscCompressor::BloscLZ,
        9.try_into().unwrap(),
        None,
        BloscShuffleMode::BitShuffle,
        Some(2),
    )
    .unwrap();

    for size in [32, 64, 128, 256, 512].iter() {
        let size3 = size * size * size;
        let rep = BytesRepresentation::FixedSize(size3);

        let data_decoded: Vec<u8> = (0..size3).map(|i| i as u8).collect();
        let data_encoded = codec.encode(data_decoded.clone()).unwrap();
        group.throughput(Throughput::Bytes(size3));
        group.bench_function(BenchmarkId::new("encode", size3), |b| {
            b.iter(|| codec.encode(data_decoded.clone()).unwrap());
        });
        group.bench_function(BenchmarkId::new("decode", size3), |b| {
            b.iter(|| codec.decode(data_encoded.clone(), &rep).unwrap());
        });
    }
}

criterion_group!(benches, codec_bytes, codec_blosc);
criterion_main!(benches);
