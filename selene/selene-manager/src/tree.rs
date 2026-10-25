use crate::{Key, Result, invalid};
use std::collections::HashMap;

#[derive(Clone, Debug)]
pub struct Node<L> {
    pub key: Key,
    pub parent: Option<Key>,
    pub bucket: [u8; 16],
    pub tail_pages: usize,
    pub row: bool,
    pub location: L,
    children: u32,
    pins: u32,
    accessed: u64,
}

impl<L> Node<L> {
    pub fn accessed(&self) -> u64 {
        self.accessed
    }
    pub fn children(&self) -> u32 {
        self.children
    }
    pub fn pinned(&self) -> bool {
        self.pins > 0
    }
}

#[derive(Debug)]
pub enum Eviction<L> {
    Leaves(Vec<Node<L>>),
    Compacted(Key),
}

pub struct Tree<L> {
    page_tokens: u64,
    nodes: HashMap<Key, Node<L>>,
}

impl<L> Tree<L> {
    pub fn new(page_tokens: u64) -> Self {
        Self {
            page_tokens,
            nodes: HashMap::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    pub fn node(&self, key: &Key) -> Option<&Node<L>> {
        self.nodes.get(key)
    }

    pub fn node_mut(&mut self, key: &Key) -> Option<&mut Node<L>> {
        self.nodes.get_mut(key)
    }

    pub fn nodes(&self) -> impl Iterator<Item = &Node<L>> {
        self.nodes.values()
    }

    #[allow(clippy::too_many_arguments)]
    pub fn insert(
        &mut self,
        key: Key,
        parent: Option<Key>,
        bucket: [u8; 16],
        tail_pages: usize,
        row: bool,
        location: L,
    ) -> Result<()> {
        if key.consumed_tokens == 0 || self.nodes.contains_key(&key) {
            return Err(invalid("host record key is empty or already indexed"));
        }
        let parent_tokens = parent.map_or(0, |key| key.consumed_tokens);
        if parent.is_some_and(|key| !self.nodes.contains_key(&key))
            || key.consumed_tokens <= parent_tokens
            || key.consumed_tokens / self.page_tokens - parent_tokens / self.page_tokens
                != tail_pages as u64
        {
            return Err(invalid("record parent or page tail"));
        }
        if let Some(parent) = parent {
            self.nodes.get_mut(&parent).unwrap().children += 1;
        }
        self.nodes.insert(
            key,
            Node {
                key,
                parent,
                bucket,
                tail_pages,
                row,
                location,
                children: 0,
                pins: 0,
                accessed: 0,
            },
        );
        Ok(())
    }

    pub fn access_at(&mut self, key: &Key, tick: u64) {
        if let Some(node) = self.nodes.get_mut(key) {
            node.accessed = tick;
        }
    }
    pub fn pin(&mut self, key: &Key) -> Result<()> {
        let node = self
            .nodes
            .get_mut(key)
            .ok_or_else(|| invalid("cannot pin an unindexed host record"))?;
        node.pins = node
            .pins
            .checked_add(1)
            .ok_or_else(|| invalid("host record pin count exhausted"))?;
        Ok(())
    }

    pub fn unpin(&mut self, key: &Key) -> Result<()> {
        let node = self
            .nodes
            .get_mut(key)
            .ok_or_else(|| invalid("cannot unpin an unindexed host record"))?;
        node.pins = node
            .pins
            .checked_sub(1)
            .ok_or_else(|| invalid("host record has no pin to release"))?;
        Ok(())
    }

    fn remove(&mut self, key: &Key) -> Result<Vec<Node<L>>> {
        let node = self
            .nodes
            .get(key)
            .ok_or_else(|| invalid("cannot remove an unindexed host record"))?;
        if node.children != 0 || node.pinned() {
            return Err(invalid(
                "cannot remove a pinned host record or one with children",
            ));
        }
        let mut removed = Vec::new();
        let mut next = Some(*key);
        while let Some(key) = next {
            let node = self.nodes.remove(&key).unwrap();
            next = None;
            if let Some(parent) = node.parent {
                let parent_node = self.nodes.get_mut(&parent).unwrap();
                parent_node.children -= 1;
                if parent_node.children == 0 && !parent_node.row && !parent_node.pinned() {
                    next = Some(parent);
                }
            }
            removed.push(node);
        }
        Ok(removed)
    }

    /// Removes exactly this node, which must have no children and no pin. Unlike `remove`, an
    /// ancestor left childless is not touched.
    pub fn remove_node(&mut self, key: &Key) -> Result<Node<L>> {
        let node = self
            .nodes
            .get(key)
            .ok_or_else(|| invalid("cannot remove an unindexed host record"))?;
        if node.children != 0 || node.pinned() {
            return Err(invalid(
                "cannot remove a pinned host record or one with children",
            ));
        }
        let node = self.nodes.remove(key).unwrap();
        if let Some(parent) = node.parent {
            if let Some(parent_node) = self.nodes.get_mut(&parent) {
                parent_node.children = parent_node.children.saturating_sub(1);
            }
        }
        Ok(node)
    }

    pub fn evict(
        &mut self,
        need_row: bool,
        evictable: impl Fn(&Node<L>) -> bool,
    ) -> Option<Eviction<L>> {
        let candidate = self
            .nodes
            .values()
            .filter(|node| {
                !node.pinned()
                    && evictable(node)
                    && (node.children == 0 || (need_row && node.row && node.children == 1))
            })
            .min_by_key(|node| (node.accessed, node.key.consumed_tokens))
            .map(|node| node.key)?;
        if self.nodes[&candidate].children == 0 {
            return self.remove(&candidate).ok().map(Eviction::Leaves);
        }
        self.nodes.get_mut(&candidate).unwrap().row = false;
        Some(Eviction::Compacted(candidate))
    }
}

impl<L> std::ops::Index<&Key> for Tree<L> {
    type Output = Node<L>;
    fn index(&self, key: &Key) -> &Self::Output {
        &self.nodes[key]
    }
}
impl<L> std::ops::IndexMut<&Key> for Tree<L> {
    fn index_mut(&mut self, key: &Key) -> &mut Self::Output {
        self.nodes.get_mut(key).unwrap()
    }
}
