use std::fs::File;
use std::io::{BufRead, BufReader};
use std::str::FromStr;

pub enum Delimiter {
    Space,
    Comma,
}

/// A type alias for a vertex and it's neighbors
type VertexStreamEntry<T> = (T, Vec<T>);

/// A pull-based vertex-stream. Consumers call `next_entry()` until None
pub struct VertexStream<T> {
    inner: Box<dyn Iterator<Item = VertexStreamEntry<T>>>,
}

impl<T> VertexStream<T>
where
    T: 'static + FromStr + Clone,
    <T as FromStr>::Err: std::fmt::Debug,
{
    pub fn from_csv(path: &str, delimiter: Delimiter) -> std::io::Result<Self> {
        let file = File::open(path)?;
        let reader = BufReader::new(file);
        let delim = delimiter;

        let iter = reader.lines().filter_map(move |line| {
            let line = line.ok()?.trim().to_string();
            if line.is_empty() {
                return None;
            }

            let items: Vec<&str> = match delim {
                Delimiter::Space => line.split_whitespace().collect(),
                Delimiter::Comma => line.split(',').map(|s| s.trim()).collect(),
            };

            if items.is_empty() {
                return None;
            }

            let v = items[0].parse::<T>().expect("Failed to parse vertex ID");
            let nbrs = items[1..]
                .iter()
                .map(|s| s.parse::<T>().expect("Failed to parse neighbors"))
                .collect();

            Some((v, nbrs))
        });

        Ok(Self {
            inner: Box::new(iter),
        })
    }

    pub fn from_adjacency_list(data: Vec<VertexStreamEntry<T>>) -> Self {
        Self {
            inner: Box::new(data.into_iter()),
        }
    }
}

impl<T> Iterator for VertexStream<T> {
    type Item = VertexStreamEntry<T>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}
