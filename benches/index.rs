use criterion::{Bencher, Criterion, criterion_group, criterion_main};

criterion_group!(benchmark, index);
criterion_main!(benchmark);

fn index(c: &mut Criterion) {
	use nosqlite::{Connection, field, Filter, json, Key};

	let connection = Connection::in_memory().unwrap();
	let table = connection.table("benchmark").unwrap();

	for i in 0..1_000 {
		table.insert(json!({ "id": i, "name": "benchmark", "foo": "bar", "bar": "baz"}), &connection)
			.unwrap();
	}

	let query = |b: &mut Bencher| b.iter(|| {
		table.iter().filter(field("id").gt(550).and(field("id").lt(600)))
			.field::<String, _>("name", &connection).unwrap().len();
	});

	c.bench_function("No Index", query);

	table.index("bench_index", &[field("id")], &connection).unwrap();
	c.bench_function("With Index", query);
}
