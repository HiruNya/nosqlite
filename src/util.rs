//! A module for utility structs that don't do much on their own

use rusqlite::types::{FromSql, ToSql};

/// A struct that represents AND.
pub struct And<A, B> {
	/// The first struct to be used.
	pub first: A,
	/// The second struct to be used.
	pub second: B,
}

/// A struct that represents OR.
pub struct Or<A, B> {
	/// The first struct to be used.
	pub first: A,
	/// The second struct to be used.
	pub second: B,
}

/// A struct that represents NOT.
pub struct Not<A>(pub A);

/// A struct that represents equality.
pub struct Eq<A, B> {
	/// The variable that is being checked/set.
	pub variable: A,
	/// The value that is being checked for/set.
	pub value: B,
}

/// A struct that represents inequality.
pub struct Neq<A, B> {
	/// The variable that is being checked/set.
	pub variable: A,
	/// The value that is being checked for/set.
	pub value: B,
}

/// A struct that compares whether `G > L`.
pub struct Gt<G, L> {
	/// The greater value.
	pub greater: G,
	/// The lesser value.
	pub lesser: L,
}

/// A struct that compares whether `G >= L`.
pub struct Gte<G, L> {
	/// The greater value.
	pub greater: G,
	/// The lesser or equal value.
	pub lesser: L,
}

/// A struct that compares using the SQL `LIKE` comparison.
pub struct Like<A, S: std::fmt::Display> {
	/// The variable to be compared.
	pub variable: A,
	/// Whether to match the start.
	pub matches_start: bool,
	/// The value to compare.
	///
	/// You can also put `%` characters into the string to match the middle.
	pub value: S,
	/// Whether to match the end.
	pub matches_end: bool,
}

/// A struct that checks whether a field exists and if that field is not null.
pub struct Exists<A>(pub A);

/// The order which the key will be sorted by
pub enum SortOrder<T> {
	/// Lowest value first
	Ascending(T),
	/// Largest value first
	Descending(T),
}
impl<T: crate::Key> SortOrder<T> {
	pub(crate) fn key(&self, data_key: &str) -> String {
		let (mut key, ascending) = match self {
			SortOrder::Ascending(k) => (k.key(data_key), true),
			SortOrder::Descending(k) => (k.key(data_key), false),
		};
		if ascending { key.push_str(" ASC") } else { key.push_str(" DESC") }
		key
	}
}
impl<T> std::ops::Not for SortOrder<T> {
	type Output = SortOrder<T>;

	fn not(self) -> Self::Output {
		match self {
			SortOrder::Ascending(t) => SortOrder::Descending(t),
			SortOrder::Descending(t) => SortOrder::Ascending(t),
		}
	}
}

/// This trait means that the type can be used as a SQL type.
pub trait SqlType: FromSql + ToSql {
	/// What this type is in SQL as a `&str`.
	///
	/// e.g. an i64 in SQL is `INTEGER`.
	fn sql_type() -> &'static str;
}

macro_rules! impl_sqltype {
    ($typ:ty, $sql:literal) => {
        impl SqlType for $typ {
            fn sql_type() -> &'static str { $sql }
        }
    };
}
impl_sqltype!(i64, "INTEGER");
impl_sqltype!(i32, "INTEGER");
impl_sqltype!(i16, "INTEGER");
impl_sqltype!(i8, "INTEGER");
impl_sqltype!(u32, "INTEGER");
impl_sqltype!(u16, "INTEGER");
impl_sqltype!(u8, "INTEGER");
impl_sqltype!(String, "TEXT");
