use base64::engine::general_purpose::STANDARD as BASE64_ENCODER;
use base64::engine::Engine as _;
#[cfg(test)]
use std::cell::Cell;
use std::io::Write;

use super::parser::{DecodedImage, KittyError, KittyErrorCode, KittyFormat};
use crate::panes::sixel::PixelRect;
use std::collections::HashMap;

pub const DEFAULT_KITTY_STORE_QUOTA_BYTES: usize = 335_544_320;
/// Distinct scaled rasters kept for one image (one source rectangle at one
/// destination pixel size each). Storing another past this drops the
/// least-recently-used other variant of that image. The byte quota can drop
/// variants sooner. Replacing a stored variant does not grow the set. A
/// remembered refusal occupies a slot but is not a raster.
pub(crate) const MAX_SCALED_VARIANTS_PER_IMAGE: usize = 4;

pub type InternalImageId = u64;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ScaledImageKey {
    pub source: PixelRect,
    pub size: (usize, usize),
}

#[derive(Debug, Clone, PartialEq)]
pub struct KittyImage {
    pub rgba: Vec<u8>,
    pub width: u32,
    pub height: u32,
}

#[derive(Debug, Clone)]
struct ScaledVariant {
    bytes: Vec<u8>,
    lru_stamp: u64,
    /// The destination was refused. `bytes` is empty; the entry only stops
    /// the same key from being scaled again. It still occupies a variant slot.
    refused: bool,
}

#[derive(Debug, Clone)]
struct StoredImage {
    image: KittyImage,
    refcount: usize,
    lru_stamp: u64,
    scaled_variants: HashMap<ScaledImageKey, ScaledVariant>,
    base64_cache: HashMap<(Option<ScaledImageKey>, bool), String>,
    /// Successful `scale_rgba` calls for this image. Reusing a stored size
    /// does not increment it.
    #[cfg(test)]
    scale_count: Cell<u64>,
}

#[derive(Debug, Clone)]
pub struct KittyImageStore {
    images: HashMap<InternalImageId, StoredImage>,
    next_image_id: InternalImageId,
    next_lru_stamp: u64,
    quota_bytes: usize,
    total_bytes: usize,
    next_placement_uid: u64,
}

impl Default for KittyImageStore {
    fn default() -> Self {
        Self::with_quota(DEFAULT_KITTY_STORE_QUOTA_BYTES)
    }
}

impl KittyImageStore {
    pub fn with_quota(quota_bytes: usize) -> Self {
        KittyImageStore {
            images: HashMap::new(),
            next_image_id: 1,
            next_lru_stamp: 0,
            quota_bytes,
            total_bytes: 0,
            next_placement_uid: 1,
        }
    }
    pub fn next_placement_uid(&mut self) -> u64 {
        let uid = self.next_placement_uid;
        self.next_placement_uid += 1;
        uid
    }
    pub fn store_image(&mut self, image: DecodedImage) -> Result<InternalImageId, KittyError> {
        let (rgba, width, height) = Self::normalize_to_rgba(image);
        let size = rgba.len();
        if size > self.quota_bytes {
            return Err(KittyError {
                code: KittyErrorCode::Einval,
                message: "image exceeds storage quota".to_owned(),
                image_id: None,
                image_number: None,
                placement_id: None,
                quiet: 0,
            });
        }
        self.evict_to_fit(size, None);
        if self.total_bytes + size > self.quota_bytes {
            return Err(KittyError {
                code: KittyErrorCode::Einval,
                message: "image storage quota exceeded".to_owned(),
                image_id: None,
                image_number: None,
                placement_id: None,
                quiet: 0,
            });
        }
        let id = self.next_image_id;
        self.next_image_id += 1;
        let lru_stamp = self.bump_lru();
        self.images.insert(
            id,
            StoredImage {
                image: KittyImage {
                    rgba,
                    width,
                    height,
                },
                refcount: 0,
                lru_stamp,
                scaled_variants: HashMap::new(),
                base64_cache: HashMap::new(),
                #[cfg(test)]
                scale_count: Cell::new(0),
            },
        );
        self.total_bytes += size;
        Ok(id)
    }
    pub fn get(&self, id: InternalImageId) -> Option<&KittyImage> {
        self.images.get(&id).map(|stored| &stored.image)
    }
    /// Pure cache lookup. A remembered refusal is not a stored raster.
    pub fn has_scaled_variant(&self, id: InternalImageId, key: ScaledImageKey) -> bool {
        self.images
            .get(&id)
            .and_then(|stored| stored.scaled_variants.get(&key))
            .is_some_and(|variant| !variant.refused)
    }
    pub fn scaled_variant(&self, id: InternalImageId, key: ScaledImageKey) -> Option<&[u8]> {
        self.images
            .get(&id)
            .and_then(|stored| stored.scaled_variants.get(&key))
            .filter(|variant| !variant.refused)
            .map(|variant| variant.bytes.as_slice())
    }
    pub(crate) fn scale_was_refused(&self, id: InternalImageId, key: ScaledImageKey) -> bool {
        self.images
            .get(&id)
            .and_then(|stored| stored.scaled_variants.get(&key))
            .is_some_and(|variant| variant.refused)
    }
    /// Quota share for one scaled raster: four of these, plus the base image, fit.
    pub(crate) fn max_synchronous_scaled_bytes(&self, id: InternalImageId) -> usize {
        let image_bytes = self
            .images
            .get(&id)
            .map(|stored| stored.image.rgba.len())
            .unwrap_or(0);
        self.quota_bytes.saturating_sub(image_bytes) / MAX_SCALED_VARIANTS_PER_IMAGE
    }
    pub(crate) fn scaled_bytes_are_retainable(
        &self,
        id: InternalImageId,
        width: usize,
        height: usize,
    ) -> bool {
        width
            .checked_mul(height)
            .and_then(|pixels| pixels.checked_mul(4))
            .is_some_and(|bytes| bytes <= self.max_synchronous_scaled_bytes(id))
    }
    /// Remember a destination that must not be scaled. The marker is not a raster.
    pub(crate) fn remember_refused_scale(&mut self, id: InternalImageId, key: ScaledImageKey) {
        if self.scale_was_refused(id, key) || self.has_scaled_variant(id, key) {
            self.touch_scaled_variant(id, key);
            return;
        }
        let lru_stamp = self.bump_lru();
        let inserted = self.images.get_mut(&id).is_some_and(|stored| {
            stored.lru_stamp = lru_stamp;
            stored.scaled_variants.insert(
                key,
                ScaledVariant {
                    bytes: Vec::new(),
                    lru_stamp,
                    refused: true,
                },
            );
            true
        });
        if inserted {
            self.drop_scaled_variants_over_cap(id, key);
        }
    }
    /// Mark a stored variant recently used without copying or rescaling it.
    pub(crate) fn touch_scaled_variant(&mut self, id: InternalImageId, key: ScaledImageKey) {
        let present = self
            .images
            .get(&id)
            .is_some_and(|stored| stored.scaled_variants.contains_key(&key));
        if !present {
            return;
        }
        let stamp = self.bump_lru();
        if let Some(stored) = self.images.get_mut(&id) {
            stored.lru_stamp = stamp;
            if let Some(variant) = stored.scaled_variants.get_mut(&key) {
                variant.lru_stamp = stamp;
            }
        }
    }
    #[cfg(test)]
    pub(crate) fn note_scale(&self, id: InternalImageId) {
        if let Some(stored) = self.images.get(&id) {
            stored
                .scale_count
                .set(stored.scale_count.get().saturating_add(1));
        }
    }
    #[cfg(test)]
    pub(crate) fn scale_count(&self, id: InternalImageId) -> u64 {
        self.images
            .get(&id)
            .map(|stored| stored.scale_count.get())
            .unwrap_or(0)
    }
    pub fn touch(&mut self, id: InternalImageId) {
        let lru_stamp = self.bump_lru();
        if let Some(stored) = self.images.get_mut(&id) {
            stored.lru_stamp = lru_stamp;
        }
    }
    pub fn add_placement_ref(&mut self, id: InternalImageId) {
        let lru_stamp = self.bump_lru();
        if let Some(stored) = self.images.get_mut(&id) {
            stored.refcount += 1;
            stored.lru_stamp = lru_stamp;
        }
    }
    pub fn remove_placement_ref(&mut self, id: InternalImageId) {
        if let Some(stored) = self.images.get_mut(&id) {
            stored.refcount = stored.refcount.saturating_sub(1);
        }
    }
    pub fn free(&mut self, id: InternalImageId) {
        let should_remove = match self.images.get_mut(&id) {
            Some(stored) => {
                stored.refcount = stored.refcount.saturating_sub(1);
                stored.refcount == 0
            },
            None => false,
        };
        if should_remove {
            self.remove_entry(id);
        }
    }
    pub fn base64_for(
        &mut self,
        id: InternalImageId,
        variant: Option<ScaledImageKey>,
        compressed: bool,
    ) -> Option<String> {
        let stored = self.images.get_mut(&id)?;
        if let Some(cached) = stored.base64_cache.get(&(variant, compressed)) {
            return Some(cached.clone());
        }
        let bytes = match variant {
            Some(key) => {
                let cached = stored.scaled_variants.get(&key)?;
                if cached.refused {
                    return None;
                }
                cached.bytes.as_slice()
            },
            None => stored.image.rgba.as_slice(),
        };
        let encoded = if compressed {
            let mut encoder = flate2::write::ZlibEncoder::new(
                Vec::with_capacity(bytes.len() / 8),
                flate2::Compression::fast(),
            );
            let deflated = encoder
                .write_all(bytes)
                .and_then(|_| encoder.finish())
                .ok()?;
            BASE64_ENCODER.encode(&deflated)
        } else {
            BASE64_ENCODER.encode(bytes)
        };
        stored
            .base64_cache
            .insert((variant, compressed), encoded.clone());
        Some(encoded)
    }
    pub fn add_scaled_variant(&mut self, id: InternalImageId, key: ScaledImageKey, bytes: Vec<u8>) {
        let lru_stamp = self.bump_lru();
        let inserted = match self.images.get_mut(&id) {
            Some(stored) => {
                let new_len = bytes.len();
                let old_len = stored
                    .scaled_variants
                    .insert(
                        key,
                        ScaledVariant {
                            bytes,
                            lru_stamp,
                            refused: false,
                        },
                    )
                    .map(|old| old.bytes.len())
                    .unwrap_or(0);
                stored
                    .base64_cache
                    .retain(|(cached_variant, _), _| *cached_variant != Some(key));
                stored.lru_stamp = lru_stamp;
                Some((old_len, new_len))
            },
            None => None,
        };
        if let Some((old_len, new_len)) = inserted {
            self.total_bytes = self.total_bytes - old_len + new_len;
            self.drop_scaled_variants_over_cap(id, key);
            if self.total_bytes > self.quota_bytes {
                self.evict_to_fit(0, Some(id));
            }
        }
    }
    pub fn reserve_scaled_variant(
        &mut self,
        id: InternalImageId,
        key: ScaledImageKey,
        bytes: usize,
    ) -> bool {
        if bytes > self.quota_bytes {
            return false;
        }
        let existing = self
            .images
            .get(&id)
            .and_then(|stored| stored.scaled_variants.get(&key))
            .filter(|variant| !variant.refused)
            .map(|variant| variant.bytes.len())
            .unwrap_or(0);
        let incoming = bytes.saturating_sub(existing);
        self.evict_to_fit(incoming, Some(id));
        self.total_bytes + incoming <= self.quota_bytes
    }
    /// Drop least-recently-used variants of `id` other than `keep` until the
    /// per-image cap holds. `keep` is the variant just stored.
    fn drop_scaled_variants_over_cap(&mut self, id: InternalImageId, keep: ScaledImageKey) {
        while self
            .images
            .get(&id)
            .is_some_and(|stored| stored.scaled_variants.len() > MAX_SCALED_VARIANTS_PER_IMAGE)
        {
            let victim = self.images.get(&id).and_then(|stored| {
                stored
                    .scaled_variants
                    .iter()
                    .filter(|(key, _)| **key != keep)
                    .min_by_key(|(_, variant)| variant.lru_stamp)
                    .map(|(key, _)| *key)
            });
            let Some(victim) = victim else {
                break;
            };
            self.discard_scaled_variant(id, victim);
        }
    }
    fn discard_scaled_variant(&mut self, id: InternalImageId, key: ScaledImageKey) {
        let freed = match self.images.get_mut(&id) {
            Some(stored) => {
                let freed = stored
                    .scaled_variants
                    .remove(&key)
                    .map(|variant| variant.bytes.len())
                    .unwrap_or(0);
                stored
                    .base64_cache
                    .retain(|(cached_variant, _), _| *cached_variant != Some(key));
                freed
            },
            None => 0,
        };
        self.total_bytes = self.total_bytes.saturating_sub(freed);
    }
    pub fn clear_scaled_variants(&mut self, id: InternalImageId) {
        if let Some(stored) = self.images.get_mut(&id) {
            self.total_bytes -= stored
                .scaled_variants
                .drain()
                .map(|(_, variant)| variant.bytes.len())
                .sum::<usize>();
            stored
                .base64_cache
                .retain(|(cached_variant, _), _| cached_variant.is_none());
        }
    }
    pub fn refcount(&self, id: InternalImageId) -> Option<usize> {
        self.images.get(&id).map(|stored| stored.refcount)
    }
    pub fn total_bytes(&self) -> usize {
        self.total_bytes
    }
    pub fn image_count(&self) -> usize {
        self.images.len()
    }
    fn bump_lru(&mut self) -> u64 {
        self.next_lru_stamp += 1;
        self.next_lru_stamp
    }
    fn stored_image_bytes(stored: &StoredImage) -> usize {
        stored.image.rgba.len()
            + stored
                .scaled_variants
                .values()
                .map(|variant| variant.bytes.len())
                .sum::<usize>()
    }
    fn remove_entry(&mut self, id: InternalImageId) {
        if let Some(stored) = self.images.remove(&id) {
            self.total_bytes -= Self::stored_image_bytes(&stored);
        }
    }
    fn evict_to_fit(&mut self, incoming_bytes: usize, protected: Option<InternalImageId>) {
        while self.total_bytes + incoming_bytes > self.quota_bytes {
            let candidate = self
                .images
                .iter()
                .filter(|(id, stored)| stored.refcount == 0 && Some(**id) != protected)
                .min_by_key(|(_, stored)| stored.lru_stamp)
                .map(|(id, _)| *id);
            match candidate {
                Some(id) => self.remove_entry(id),
                None => break,
            }
        }
    }
    fn normalize_to_rgba(image: DecodedImage) -> (Vec<u8>, u32, u32) {
        match image.format {
            KittyFormat::Rgb24 => {
                let mut rgba = Vec::with_capacity(image.bytes.len() / 3 * 4);
                for pixel in image.bytes.chunks_exact(3) {
                    rgba.extend_from_slice(pixel);
                    rgba.push(255);
                }
                (rgba, image.width, image.height)
            },
            KittyFormat::Rgba32 | KittyFormat::Png => (image.bytes, image.width, image.height),
        }
    }
}

#[cfg(test)]
#[path = "./unit/store_tests.rs"]
mod store_tests;
