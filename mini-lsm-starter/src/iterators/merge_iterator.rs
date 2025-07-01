// Copyright (c) 2022-2025 Alex Chi Z
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::collections::binary_heap::PeekMut;

use anyhow::Result;

use super::StorageIterator;
use crate::key::KeySlice;

struct HeapWrapper<I: StorageIterator>(pub usize, pub Box<I>);

impl<I: StorageIterator> PartialEq for HeapWrapper<I> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl<I: StorageIterator> Eq for HeapWrapper<I> {}

impl<I: StorageIterator> PartialOrd for HeapWrapper<I> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<I: StorageIterator> Ord for HeapWrapper<I> {
    fn cmp(&self, other: &Self) -> Ordering {
        // we want to pop the smallest key first, tie‐break on index,
        // so reverse the natural order for a max‐heap
        self.1
            .key()
            .cmp(&other.1.key())
            .then(self.0.cmp(&other.0))
            .reverse()
    }
}

/// Merge multiple StorageIterator<I> (all sorted by key).  If the same key appears in more
/// than one iterator, the one with the smaller index wins.
pub struct MergeIterator<I: StorageIterator> {
    iters: BinaryHeap<HeapWrapper<I>>,
    current: Option<HeapWrapper<I>>,
}

impl<I: StorageIterator> MergeIterator<I> {
    pub fn create(iters: Vec<Box<I>>) -> Self {
        // 1) No iterators at all
        if iters.is_empty() {
            return Self {
                iters: BinaryHeap::new(),
                current: None,
            };
        }

        // 2) All iterators invalid?  Just pick the last one and call it “current”
        let all_invalid = iters.iter().all(|it| !it.is_valid());
        if all_invalid {
            let mut mut_iters = iters;
            return Self {
                iters: BinaryHeap::new(),
                current: Some(HeapWrapper(0, mut_iters.pop().unwrap())),
            };
        }

        // 3) Some valid: push each valid one into the heap
        let mut heap = BinaryHeap::new();
        for (idx, it) in iters.into_iter().enumerate() {
            if it.is_valid() {
                heap.push(HeapWrapper(idx, it));
            }
        }
        // Pop the smallest key as the initial “current”
        let current = heap.pop().unwrap();
        Self {
            iters: heap,
            current: Some(current),
        }
    }
}

impl<I: 'static + for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>> StorageIterator
    for MergeIterator<I>
{
    type KeyType<'a> = KeySlice<'a>;

    fn key(&self) -> KeySlice {
        self.current.as_ref().unwrap().1.key()
    }

    fn value(&self) -> &[u8] {
        self.current.as_ref().unwrap().1.value()
    }

    fn is_valid(&self) -> bool {
        self.current
            .as_ref()
            .map(|hw| hw.1.is_valid())
            .unwrap_or(false)
    }

    fn next(&mut self) -> Result<()> {
        // 1) Advance any other iterators at the same key
        let mut_current = self.current.as_mut().unwrap();
        while let Some(mut top) = self.iters.peek_mut() {
            if top.1.key() == mut_current.1.key() {
                // pop & advance the duplicate
                if let Err(e) = top.1.next() {
                    PeekMut::pop(top);
                    return Err(e);
                }
                if !top.1.is_valid() {
                    PeekMut::pop(top);
                }
            } else {
                break;
            }
        }

        // 2) Advance the current iterator
        mut_current.1.next()?;
        if !mut_current.1.is_valid() {
            // it ended: swap in the heap-top as the new current
            if let Some(next) = self.iters.pop() {
                *mut_current = next;
            }
            return Ok(());
        }

        // 3) Re-balance: if the heap-top has a smaller key, swap
        if let Some(mut top) = self.iters.peek_mut() {
            if *mut_current < *top {
                std::mem::swap(&mut *top, mut_current);
            }
        }

        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        self.iters.len() + if self.current.is_some() { 1 } else { 0 }
    }
}
