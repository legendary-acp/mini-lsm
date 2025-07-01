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

mod leveled;
mod simple_leveled;
mod tiered;

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Ok, Result};
pub use leveled::{LeveledCompactionController, LeveledCompactionOptions, LeveledCompactionTask};
use serde::{Deserialize, Serialize};
pub use simple_leveled::{
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, SimpleLeveledCompactionTask,
};
pub use tiered::{TieredCompactionController, TieredCompactionOptions, TieredCompactionTask};

use crate::iterators::StorageIterator;
use crate::iterators::concat_iterator::SstConcatIterator;
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::lsm_storage::{LsmStorageInner, LsmStorageState};
use crate::table::{SsTable, SsTableBuilder, SsTableIterator};

#[derive(Debug, Serialize, Deserialize)]
pub enum CompactionTask {
    Leveled(LeveledCompactionTask),
    Tiered(TieredCompactionTask),
    Simple(SimpleLeveledCompactionTask),
    ForceFullCompaction {
        l0_sstables: Vec<usize>,
        l1_sstables: Vec<usize>,
    },
}

impl CompactionTask {
    fn compact_to_bottom_level(&self) -> bool {
        match self {
            CompactionTask::ForceFullCompaction { .. } => true,
            CompactionTask::Leveled(task) => task.is_lower_level_bottom_level,
            CompactionTask::Simple(task) => task.is_lower_level_bottom_level,
            CompactionTask::Tiered(task) => task.bottom_tier_included,
        }
    }
}

pub(crate) enum CompactionController {
    Leveled(LeveledCompactionController),
    Tiered(TieredCompactionController),
    Simple(SimpleLeveledCompactionController),
    NoCompaction,
}

impl CompactionController {
    pub fn generate_compaction_task(&self, snapshot: &LsmStorageState) -> Option<CompactionTask> {
        match self {
            CompactionController::Leveled(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Leveled),
            CompactionController::Simple(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Simple),
            CompactionController::Tiered(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Tiered),
            CompactionController::NoCompaction => unreachable!(),
        }
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &CompactionTask,
        output: &[usize],
        in_recovery: bool,
    ) -> (LsmStorageState, Vec<usize>) {
        match (self, task) {
            (CompactionController::Leveled(ctrl), CompactionTask::Leveled(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output, in_recovery)
            }
            (CompactionController::Simple(ctrl), CompactionTask::Simple(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            (CompactionController::Tiered(ctrl), CompactionTask::Tiered(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            _ => unreachable!(),
        }
    }
}

impl CompactionController {
    pub fn flush_to_l0(&self) -> bool {
        matches!(
            self,
            Self::Leveled(_) | Self::Simple(_) | Self::NoCompaction
        )
    }
}

#[derive(Debug, Clone)]
pub enum CompactionOptions {
    /// Leveled compaction with partial compaction + dynamic level support (= RocksDB's Leveled
    /// Compaction)
    Leveled(LeveledCompactionOptions),
    /// Tiered compaction (= RocksDB's universal compaction)
    Tiered(TieredCompactionOptions),
    /// Simple leveled compaction
    Simple(SimpleLeveledCompactionOptions),
    /// In no compaction mode (week 1), always flush to L0
    NoCompaction,
}

impl LsmStorageInner {
    fn simple_task_compact(
        &self,
        upper_level_ids: Vec<usize>,
        lower_level_ids: Vec<usize>,
        is_bottom_level: bool,
    ) -> Result<Vec<Arc<SsTable>>> {
        let mut res_vec = Vec::new();
        let snapshot = {
            let guard = self.state.read();
            guard.as_ref().clone()
        };
        let upper_level_ssts: Vec<Arc<SsTable>> = upper_level_ids
            .iter()
            .map(|id| snapshot.sstables.get(id).unwrap().clone())
            .collect();
        let lower_level_ssts: Vec<Arc<SsTable>> = lower_level_ids
            .iter()
            .map(|id| snapshot.sstables.get(id).unwrap().clone())
            .collect();

        let mut upper_iters = Vec::new();
        upper_level_ssts.iter().for_each(|sst| {
            upper_iters.push(Box::new(
                SsTableIterator::create_and_seek_to_first(sst.clone()).unwrap(),
            ));
        });
        let upper_merge_iter = MergeIterator::create(upper_iters);
        let lower_concat_iter = SstConcatIterator::create_and_seek_to_first(lower_level_ssts)?;
        let mut two_level_merge_iter =
            TwoMergeIterator::create(upper_merge_iter, lower_concat_iter)?;
        let mut sst_builder = SsTableBuilder::new(self.options.block_size);
        while two_level_merge_iter.is_valid() {
            if two_level_merge_iter.value().is_empty() && is_bottom_level {
                two_level_merge_iter.next()?;
                continue;
            }
            sst_builder.add(two_level_merge_iter.key(), two_level_merge_iter.value());
            if sst_builder.estimated_size() > self.options.target_sst_size {
                let new_sst_id = self.next_sst_id();
                let new_sst = Arc::new(
                    sst_builder
                        .build(
                            new_sst_id,
                            Some(self.block_cache.clone()),
                            self.path_of_sst(new_sst_id),
                        )
                        .unwrap(),
                );
                res_vec.push(new_sst);
                sst_builder = SsTableBuilder::new(self.options.block_size);
            }
            two_level_merge_iter.next()?;
        }
        let new_sst_id = self.next_sst_id();
        let new_sst = Arc::new(
            sst_builder
                .build(
                    new_sst_id,
                    Some(self.block_cache.clone()),
                    self.path_of_sst(new_sst_id),
                )
                .unwrap(),
        );
        res_vec.push(new_sst);
        Ok(res_vec)
    }

    // Updated compact method in compact.rs
    fn compact(&self, task: &CompactionTask) -> Result<Vec<Arc<SsTable>>> {
        let snapshot = {
            let state = self.state.read();
            state.clone()
        };
        let mut res_vec = Vec::new();
        match task {
            CompactionTask::ForceFullCompaction {
                l0_sstables,
                l1_sstables,
            } => {
                let l0_ssts: Vec<Arc<SsTable>> = l0_sstables
                    .iter()
                    .map(|id| snapshot.sstables.get(id).unwrap().clone())
                    .collect();
                let l1_ssts: Vec<Arc<SsTable>> = l1_sstables
                    .iter()
                    .map(|id| snapshot.sstables.get(id).unwrap().clone())
                    .collect();
                let mut all_iters = Vec::new();
                l0_ssts.iter().for_each(|sst| {
                    all_iters.push(Box::new(
                        SsTableIterator::create_and_seek_to_first(sst.clone()).unwrap(),
                    ));
                });

                let l0_merge_iter = MergeIterator::create(all_iters);
                let l1_concat_iter = SstConcatIterator::create_and_seek_to_first(l1_ssts)?;
                let mut merge_iter = TwoMergeIterator::create(l0_merge_iter, l1_concat_iter)?;

                let mut sst_builder = SsTableBuilder::new(self.options.block_size);
                while merge_iter.is_valid() {
                    // skip deleted key here.
                    if merge_iter.value().is_empty() {
                        merge_iter.next()?;
                        continue;
                    }
                    sst_builder.add(merge_iter.key(), merge_iter.value());
                    if sst_builder.estimated_size() > self.options.target_sst_size {
                        // if add is failed, generate a new sstable.
                        let new_sst_id = self.next_sst_id();
                        let new_sst = Arc::new(
                            sst_builder
                                .build(
                                    new_sst_id,
                                    Some(self.block_cache.clone()),
                                    self.path_of_sst(new_sst_id),
                                )
                                .unwrap(),
                        );
                        res_vec.push(new_sst);
                        sst_builder = SsTableBuilder::new(self.options.block_size);
                    }
                    merge_iter.next()?;
                }
                // add the last sstable to res_vec.
                let new_sst_id = self.next_sst_id();
                let new_sst = Arc::new(
                    sst_builder
                        .build(
                            new_sst_id,
                            Some(self.block_cache.clone()),
                            self.path_of_sst(new_sst_id),
                        )
                        .unwrap(),
                );
                res_vec.push(new_sst);
            }
            CompactionTask::Simple(simple_task) => {
                let result = self.simple_task_compact(
                    simple_task.upper_level_sst_ids.clone(),
                    simple_task.lower_level_sst_ids.clone(),
                    simple_task.is_lower_level_bottom_level,
                )?;
                res_vec = result.clone();
            }
            _ => unreachable!(),
        };
        Ok(res_vec)
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        let CompactionOptions::NoCompaction = self.options.compaction_options else {
            panic!("full compaction can only be called with compaction is not enabled")
        };
        let snapshot = {
            let guard = self.state.read();
            Arc::clone(&guard)
        };
        let l0_sstable = snapshot.l0_sstables.clone();
        let l1_sstable = snapshot.levels[0].1.clone();
        let compaction_task = CompactionTask::ForceFullCompaction {
            l0_sstables: l0_sstable.clone(),
            l1_sstables: l1_sstable.clone(),
        };

        let sstables = self.compact(&compaction_task)?;
        {
            let _state_lock = self.state_lock.lock();
            let mut state = self.state.read().as_ref().clone();
            for sst in l0_sstable.iter().chain(l1_sstable.iter()) {
                let result = state.sstables.remove(sst);
                assert!(result.is_some());
            }
            let mut ids = Vec::with_capacity(sstables.len());
            for new_sst in sstables {
                ids.push(new_sst.sst_id());
                let res = state.sstables.insert(new_sst.sst_id(), new_sst);
                assert!(res.is_none());
            }
            assert_eq!(l1_sstable, state.levels[0].1);
            state.levels[0].1 = ids;
            let mut l0_sstables_map = l0_sstable.iter().copied().collect::<HashSet<_>>();
            state.l0_sstables = state
                .l0_sstables
                .iter()
                .filter(|x| !l0_sstables_map.remove(x))
                .copied()
                .collect::<Vec<_>>();
            assert!(l0_sstables_map.is_empty());
            *self.state.write() = Arc::new(state);
        }
        for sst in l0_sstable.iter().chain(l1_sstable.iter()) {
            std::fs::remove_file(self.path_of_sst(*sst))?;
        }
        Ok(())
    }

    fn trigger_compaction(&self) -> Result<()> {
        let snapshot = {
            let guard = self.state.read();
            guard.as_ref().clone()
        };
        let compaction_task = self
            .compaction_controller
            .generate_compaction_task(&snapshot);

        if compaction_task.is_none() {
            return Ok(());
        }

        // then perform the compaction according to the compaction task.

        let compaction_task = compaction_task.unwrap();

        let compacted_sstables = self.compact(&compaction_task)?;

        let output: Vec<usize> = compacted_sstables.iter().map(|sst| sst.sst_id()).collect();

        let files_to_remove: Vec<usize> = {
            let state_lock = self.state_lock.lock();

            let mut snapshot = self.state.read().as_ref().clone();

            let mut ssts_to_remove: Vec<usize> = Vec::new();

            for sst in compacted_sstables {
                snapshot.sstables.insert(sst.sst_id(), sst.clone());
            }

            let (mut snapshot, files_to_remove) = self
                .compaction_controller
                .apply_compaction_result(&snapshot, &compaction_task, &output, false);

            for file_to_remove in files_to_remove.iter() {
                snapshot.sstables.remove(file_to_remove);

                ssts_to_remove.push(*file_to_remove);
            }

            let mut write_state = self.state.write();

            *write_state = Arc::new(snapshot);

            drop(write_state);

            ssts_to_remove
        };

        for file_to_remove in files_to_remove.iter() {
            let path = self.path_of_sst(*file_to_remove);

            if let Err(e) = std::fs::remove_file(path) {
                eprintln!("remove sstable failed: {}", e);
            }
        }

        Ok(())
    }

    pub(crate) fn spawn_compaction_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        if let CompactionOptions::Leveled(_)
        | CompactionOptions::Simple(_)
        | CompactionOptions::Tiered(_) = self.options.compaction_options
        {
            let this = self.clone();
            let handle = std::thread::spawn(move || {
                let ticker = crossbeam_channel::tick(Duration::from_millis(50));
                loop {
                    crossbeam_channel::select! {
                        recv(ticker) -> _ => if let Err(e) = this.trigger_compaction() {
                            eprintln!("compaction failed: {}", e);
                        },
                        recv(rx) -> _ => return
                    }
                }
            });
            return Ok(Some(handle));
        }
        Ok(None)
    }

    fn trigger_flush(&self) -> Result<()> {
        let res = {
            let guard = self.state.read();
            guard.imm_memtables.len() >= self.options.num_memtable_limit
        };
        if res {
            self.force_flush_next_imm_memtable()?;
        }
        Ok(())
    }

    pub(crate) fn spawn_flush_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        let this = self.clone();
        let handle = std::thread::spawn(move || {
            let ticker = crossbeam_channel::tick(Duration::from_millis(50));
            loop {
                crossbeam_channel::select! {
                    recv(ticker) -> _ => if let Err(e) = this.trigger_flush() {
                        eprintln!("flush failed: {}", e);
                    },
                    recv(rx) -> _ => return
                }
            }
        });
        Ok(Some(handle))
    }
}
