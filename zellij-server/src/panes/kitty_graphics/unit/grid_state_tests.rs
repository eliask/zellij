use super::{crop_rgba, scale_rgba, KittyGrid, KittyPlacement, KittyVerticalAnchor};
use crate::panes::kitty_graphics::parser::{
    DecodedImage, KittyCommand, KittyError, KittyErrorCode, KittyFormat,
};
use crate::panes::kitty_graphics::store::KittyImageStore;
use crate::panes::sixel::PixelRect;
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use zellij_utils::pane_size::SizeInPixels;

fn rgba_pixel(value: u8) -> [u8; 4] {
    [value, value, value, 255]
}

fn raster(values: &[u8]) -> Vec<u8> {
    values
        .iter()
        .flat_map(|value| rgba_pixel(*value).to_vec())
        .collect()
}

fn test_grid() -> KittyGrid {
    KittyGrid::new(
        Rc::new(RefCell::new(Some(SizeInPixels {
            width: 10,
            height: 20,
        }))),
        Rc::new(RefCell::new(KittyImageStore::default())),
    )
}

fn test_placement(y: isize, height: usize) -> KittyPlacement {
    KittyPlacement {
        image_id: 1,
        placement_id: 0,
        placement_uid: 0,
        internal_id: 1,
        display_rect: PixelRect {
            x: 0,
            y,
            width: 20,
            height,
        },
        source_rect: PixelRect {
            x: 0,
            y: 0,
            width: 20,
            height,
        },
        emit_x: 0,
        emit_y: 0,
        scaled_px: None,
        dest_cells: (2, 2),
        cell_offset: (0, 0),
        z_index: 0,
        vertical_anchor: KittyVerticalAnchor {
            canonical_line: 0,
            offset_px_from_line_start: 0,
        },
    }
}

#[test]
fn scale_rgba_identity() {
    let src = raster(&[10, 20, 30, 40]);
    let out = scale_rgba(&src, 2, 2, 2, 2);
    assert_eq!(out, src);
}

#[test]
fn scale_rgba_upscales_constant_pixel() {
    let src = raster(&[123]);
    let out = scale_rgba(&src, 1, 1, 3, 3);
    assert_eq!(out, raster(&[123; 9]));
}

#[test]
fn scale_rgba_bilinear_gradient() {
    let src = raster(&[0, 255]);
    let out = scale_rgba(&src, 2, 1, 4, 1);
    let expected: Vec<u8> = vec![
        0, 0, 0, 255, 64, 64, 64, 255, 191, 191, 191, 255, 255, 255, 255, 255,
    ];
    assert_eq!(out, expected);
}

#[test]
fn crop_rgba_extracts_sub_rect() {
    let src = raster(&[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]);
    let out = crop_rgba(&src, 4, 4, 1, 1, 2, 2);
    assert_eq!(out, raster(&[5, 6, 9, 10]));
}

#[test]
fn changed_chunks_for_placement_straddling_changed_rect() {
    let mut grid = test_grid();
    grid.placements.push(test_placement(30, 40));
    let mut changed_rects = HashMap::new();
    changed_rects.insert(1, 1);
    let chunks = grid.changed_kitty_chunks_in_viewport(changed_rects, 0, 4, 0, 0);
    assert_eq!(chunks.len(), 1);
    let chunk = &chunks[0];
    assert_eq!(chunk.cell_x, 0);
    assert_eq!(chunk.cell_y, 1);
    assert_eq!(chunk.source_px_x, 0);
    assert_eq!(chunk.source_px_y, 0);
    assert_eq!(chunk.source_px_width, 20);
    assert_eq!(chunk.source_px_height, 10);
    assert_eq!(chunk.scaled_image, None);
}

#[test]
fn clear_visible_placements_keeps_history_resident_images() {
    let mut grid = test_grid();
    grid.placements.push(test_placement(0, 40));
    grid.placements.push(test_placement(400, 40));
    grid.clear_visible_placements(20);
    assert_eq!(grid.placement_count(), 1);
    assert_eq!(grid.placements[0].display_rect.y, 0);
}

#[test]
fn clear_all_placements_removes_history_resident_images() {
    let mut grid = test_grid();
    grid.placements.push(test_placement(0, 40));
    grid.placements.push(test_placement(400, 40));
    grid.clear_all_placements();
    assert_eq!(grid.placement_count(), 0);
}

#[test]
fn changed_chunks_for_placement_with_top_scrolled_above_viewport() {
    let mut grid = test_grid();
    grid.placements.push(test_placement(-20, 60));
    let chunks = grid.viewport_kitty_chunks(3, 0, 4, 0, 0);
    assert_eq!(chunks.len(), 1);
    let chunk = &chunks[0];
    assert_eq!(chunk.cell_x, 0);
    assert_eq!(chunk.cell_y, 0);
    assert_eq!(chunk.source_px_x, 0);
    assert_eq!(chunk.source_px_y, 20);
    assert_eq!(chunk.source_px_width, 20);
    assert_eq!(chunk.source_px_height, 40);
}

#[test]
fn changed_chunks_shift_cell_coordinates_by_viewport_offsets() {
    let mut grid = test_grid();
    let cell_width = 10usize;
    let cell_height = 20usize;
    let placement_x = 0usize;
    let placement_y = 30usize;
    grid.placements
        .push(test_placement(placement_y as isize, 40));
    let mut changed_rects = HashMap::new();
    changed_rects.insert(1, 1);
    let viewport_x_offset = 5usize;
    let viewport_y_offset = 7usize;
    let chunks = grid.changed_kitty_chunks_in_viewport(
        changed_rects,
        0,
        4,
        viewport_x_offset,
        viewport_y_offset,
    );
    assert_eq!(chunks.len(), 1);
    let chunk = &chunks[0];
    let expected_cell_x = viewport_x_offset + placement_x / cell_width;
    let expected_cell_y = viewport_y_offset + placement_y / cell_height;
    assert_eq!(chunk.cell_x, expected_cell_x);
    assert_eq!(chunk.cell_y, expected_cell_y);
    assert_eq!(chunk.source_px_x, 0);
    assert_eq!(chunk.source_px_y, 0);
    assert_eq!(chunk.source_px_width, 20);
    assert_eq!(chunk.source_px_height, 10);
}

#[test]
fn changed_chunks_for_scaled_placement_use_variant_space() {
    let mut grid = test_grid();
    let mut placement = test_placement(0, 40);
    placement.scaled_px = Some((20, 40));
    placement.dest_cells = (2, 2);
    grid.placements.push(placement);
    let mut changed_rects = HashMap::new();
    changed_rects.insert(1, 1);
    let chunks = grid.changed_kitty_chunks_in_viewport(changed_rects, 0, 4, 0, 0);
    assert_eq!(chunks.len(), 1);
    let chunk = &chunks[0];
    assert_eq!(chunk.cell_y, 1);
    assert_eq!(chunk.source_px_y, 20);
    assert_eq!(chunk.source_px_height, 20);
    assert_eq!(chunk.scaled_image.unwrap().size, (20, 40));
}

#[test]
fn changed_chunks_propagate_emit_offsets_of_clipped_placement() {
    let mut grid = test_grid();
    let mut placement = test_placement(100, 20);
    placement.emit_y = 20;
    grid.placements.push(placement);
    let mut changed_rects = HashMap::new();
    changed_rects.insert(5, 1);
    let chunks = grid.changed_kitty_chunks_in_viewport(changed_rects, 0, 4, 0, 0);
    assert_eq!(chunks.len(), 1);
    let chunk = &chunks[0];
    assert_eq!(chunk.cell_y, 5);
    assert_eq!(chunk.source_px_y, 20);
    assert_eq!(chunk.source_px_height, 20);
}

#[test]
fn region_scroll_alt_full_screen_moves_and_clips_at_top() {
    let mut grid = test_grid();
    grid.placements.push(test_placement(40, 40));
    grid.placements.push(test_placement(0, 40));
    grid.placements.push(test_placement(180, 20));
    grid.apply_region_scroll(0, 200, 20, 0, 0, false);
    assert_eq!(grid.placements.len(), 3);
    assert_eq!(grid.placements[0].display_rect.y, 20);
    assert_eq!(grid.placements[0].display_rect.height, 40);
    assert_eq!(grid.placements[0].emit_y, 0);
    assert_eq!(grid.placements[1].display_rect.y, 0);
    assert_eq!(grid.placements[1].display_rect.height, 20);
    assert_eq!(grid.placements[1].emit_y, 20);
    assert_eq!(grid.placements[2].display_rect.y, 160);
}

#[test]
fn region_scroll_top_zero_non_alt_keeps_inside_and_shifts_below() {
    let mut grid = test_grid();
    grid.placements.push(test_placement(0, 20));
    grid.placements.push(test_placement(60, 40));
    grid.placements.push(test_placement(160, 20));
    grid.apply_region_scroll(40, 140, 20, 20, 40, false);
    assert_eq!(grid.placements.len(), 3);
    assert_eq!(grid.placements[0].display_rect.y, 0);
    assert_eq!(grid.placements[1].display_rect.y, 60);
    assert_eq!(grid.placements[1].display_rect.height, 40);
    assert_eq!(grid.placements[2].display_rect.y, 180);
}

#[test]
fn region_scroll_inner_region_moves_inside_and_clips_straddler() {
    let mut grid = test_grid();
    grid.placements.push(test_placement(40, 20));
    grid.placements.push(test_placement(80, 40));
    grid.placements.push(test_placement(0, 20));
    grid.apply_region_scroll(20, 100, 20, 0, 0, false);
    assert_eq!(grid.placements.len(), 3);
    assert_eq!(grid.placements[0].display_rect.y, 20);
    assert_eq!(grid.placements[0].display_rect.height, 20);
    assert_eq!(grid.placements[1].display_rect.y, 100);
    assert_eq!(grid.placements[1].display_rect.height, 20);
    assert_eq!(grid.placements[1].emit_y, 20);
    assert_eq!(grid.placements[2].display_rect.y, 0);
}

#[test]
fn region_scroll_reaps_placement_scrolled_out_of_region() {
    let mut grid = test_grid();
    grid.placements.push(test_placement(20, 20));
    grid.apply_region_scroll(20, 100, 20, 0, 0, false);
    assert_eq!(grid.placements.len(), 0);
}

#[test]
fn reverse_index_region_scroll_clips_bottom_of_straddler() {
    let mut grid = test_grid();
    grid.placements.push(test_placement(40, 40));
    grid.apply_region_scroll(0, 80, -20, 0, 0, false);
    assert_eq!(grid.placements.len(), 1);
    let placement = &grid.placements[0];
    assert_eq!(placement.display_rect.y, 60);
    assert_eq!(placement.display_rect.height, 20);
    assert_eq!(placement.emit_y, 0);
    assert_eq!(placement.source_rect.y, 0);
}

#[test]
fn reverse_index_region_scroll_reaps_fully_below_margin() {
    let mut grid = test_grid();
    grid.placements.push(test_placement(60, 20));
    grid.apply_region_scroll(0, 80, -20, 0, 0, false);
    assert_eq!(grid.placements.len(), 0);
}

fn transmitted(
    grid: &mut KittyGrid,
    width: u32,
    height: u32,
) -> (u32, super::super::store::InternalImageId) {
    let command = KittyCommand {
        image_id: Some(1),
        pixel_width: Some(width),
        pixel_height: Some(height),
        ..Default::default()
    };
    let image = DecodedImage {
        bytes: vec![0u8; (width * height * 4) as usize],
        width,
        height,
        format: KittyFormat::Rgba32,
    };
    let pane_image_id = grid.transmit(&command, image).expect("a transmission");
    let internal = grid.pane_image_id_map()[&pane_image_id];
    (pane_image_id, internal)
}

#[test]
fn an_unscaled_placement_emits_from_its_source_rectangle() {
    let mut grid = test_grid();
    let (pane_image_id, internal) = transmitted(&mut grid, 64, 64);
    let command = KittyCommand {
        image_id: Some(1),
        source_x: 8,
        source_y: 10,
        source_w: 16,
        source_h: 20,
        ..Default::default()
    };
    let cell = SizeInPixels {
        width: 10,
        height: 20,
    };
    grid.place(
        pane_image_id,
        internal,
        &command,
        (0, 0),
        cell,
        KittyVerticalAnchor {
            canonical_line: 0,
            offset_px_from_line_start: 0,
        },
    )
    .expect("a placement");

    let mut changed_rects = HashMap::new();
    changed_rects.insert(0, 2);
    let chunks = grid.changed_kitty_chunks_in_viewport(changed_rects, 0, 40, 0, 0);
    assert_eq!(chunks.len(), 1);
    assert_eq!(chunks[0].source_px_x, 8);
    assert_eq!(chunks[0].source_px_y, 10);
    assert_eq!(chunks[0].source_px_width, 16);
    assert_eq!(chunks[0].source_px_height, 20);
}

#[test]
fn a_scaled_placement_emits_from_the_origin_of_its_cropped_variant() {
    let mut grid = test_grid();
    let (pane_image_id, internal) = transmitted(&mut grid, 64, 64);
    let command = KittyCommand {
        image_id: Some(1),
        source_x: 8,
        source_y: 10,
        source_w: 16,
        source_h: 20,
        columns: 2,
        rows: 2,
        ..Default::default()
    };
    let cell = SizeInPixels {
        width: 10,
        height: 20,
    };
    grid.place(
        pane_image_id,
        internal,
        &command,
        (0, 0),
        cell,
        KittyVerticalAnchor {
            canonical_line: 0,
            offset_px_from_line_start: 0,
        },
    )
    .expect("a placement");

    let placement = &grid.placements()[0];
    assert_eq!(placement.emit_x, 0);
    assert_eq!(placement.emit_y, 0);
    assert_eq!(placement.source_rect.x, 8);
    assert_eq!(placement.source_rect.y, 10);
}

fn place_scaled(
    grid: &mut KittyGrid,
    pane_image_id: u32,
    internal: super::super::store::InternalImageId,
    placement_id: u32,
    columns: u32,
    rows: u32,
    source_x: u32,
    source_w: u32,
) -> Result<super::super::store::ScaledImageKey, KittyError> {
    let command = KittyCommand {
        image_id: Some(pane_image_id),
        placement_id: Some(placement_id),
        columns,
        rows,
        source_x,
        source_w,
        ..Default::default()
    };
    // 8×16 pixel cells, small enough that a fitting destination stays in quota.
    let cell = SizeInPixels {
        width: 8,
        height: 16,
    };
    grid.place(
        pane_image_id,
        internal,
        &command,
        (0, 0),
        cell,
        KittyVerticalAnchor {
            canonical_line: 0,
            offset_px_from_line_start: 0,
        },
    )?;
    Ok(grid
        .placements()
        .iter()
        .find(|placement| placement.placement_id == placement_id)
        .and_then(|placement| placement.scaled_image())
        .expect("a scaled placement"))
}

fn scaled_buffer(
    grid: &KittyGrid,
    internal: super::super::store::InternalImageId,
    key: super::super::store::ScaledImageKey,
) -> Option<(usize, usize)> {
    let store = grid.kitty_image_store.borrow();
    store
        .scaled_variant(internal, key)
        .map(|bytes| (bytes.as_ptr() as usize, bytes.len()))
}

#[test]
fn repeated_place_at_the_same_cell_size_reuses_the_scaled_buffer() {
    let mut grid = test_grid();
    let (pane_image_id, internal) = transmitted(&mut grid, 1, 1);
    // 4×2 cells at 8×16 px is 32×32.
    let key = place_scaled(&mut grid, pane_image_id, internal, 1, 4, 2, 0, 0).expect("placed");
    let (ptr, len) = scaled_buffer(&grid, internal, key).expect("cached scaled raster");
    assert_eq!(len, 32 * 32 * 4);
    assert_eq!(grid.kitty_image_store.borrow().scale_count(internal), 1);

    place_scaled(&mut grid, pane_image_id, internal, 1, 4, 2, 0, 0).expect("replaced");
    let (again_ptr, again_len) =
        scaled_buffer(&grid, internal, key).expect("the cached raster is still there");
    assert_eq!(
        again_ptr, ptr,
        "the second place must not allocate another scaled buffer"
    );
    assert_eq!(again_len, len);
    assert_eq!(grid.kitty_image_store.borrow().scale_count(internal), 1);

    let other =
        place_scaled(&mut grid, pane_image_id, internal, 1, 5, 2, 0, 0).expect("other size");
    assert_ne!(other.size, key.size);
    assert_eq!(grid.kitty_image_store.borrow().scale_count(internal), 2);
    let (still_ptr, _) = scaled_buffer(&grid, internal, key).expect("the first size stays cached");
    assert_eq!(still_ptr, ptr);

    place_scaled(&mut grid, pane_image_id, internal, 1, 4, 2, 0, 0).expect("restored");
    let (restored_ptr, _) = scaled_buffer(&grid, internal, key).expect("the first size is reused");
    assert_eq!(restored_ptr, ptr);
    assert_eq!(grid.kitty_image_store.borrow().scale_count(internal), 2);
}

#[test]
fn the_same_destination_size_with_a_different_source_is_scaled_again() {
    let mut grid = test_grid();
    let (pane_image_id, internal) = transmitted(&mut grid, 4, 1);
    let first =
        place_scaled(&mut grid, pane_image_id, internal, 1, 2, 1, 0, 2).expect("first crop");
    let second =
        place_scaled(&mut grid, pane_image_id, internal, 1, 2, 1, 2, 2).expect("second crop");
    assert_eq!(first.size, second.size);
    assert_ne!(first.source, second.source);
    let (first_ptr, first_len) = scaled_buffer(&grid, internal, first).expect("first crop");
    let (second_ptr, second_len) = scaled_buffer(&grid, internal, second).expect("second crop");
    assert_eq!(first_len, second_len);
    assert_ne!(first_ptr, second_ptr);
    assert_eq!(grid.kitty_image_store.borrow().scale_count(internal), 2);
}

#[test]
fn a_new_scaled_size_drops_the_least_recently_used_variant_past_the_cap() {
    use super::super::store::MAX_SCALED_VARIANTS_PER_IMAGE;

    let mut grid = test_grid();
    let (pane_image_id, internal) = transmitted(&mut grid, 1, 1);
    let mut keys = Vec::new();
    for columns in 1..=MAX_SCALED_VARIANTS_PER_IMAGE as u32 {
        keys.push(
            place_scaled(&mut grid, pane_image_id, internal, 1, columns, 1, 0, 0)
                .expect("a distinct size"),
        );
    }
    assert_eq!(
        grid.kitty_image_store.borrow().scale_count(internal),
        MAX_SCALED_VARIANTS_PER_IMAGE as u64
    );
    for key in &keys {
        assert!(grid
            .kitty_image_store
            .borrow()
            .has_scaled_variant(internal, *key));
    }

    let extra =
        place_scaled(&mut grid, pane_image_id, internal, 1, 100, 1, 0, 0).expect("past the cap");
    assert!(
        !grid
            .kitty_image_store
            .borrow()
            .has_scaled_variant(internal, keys[0]),
        "the least-recently-used size is the one dropped"
    );
    for key in keys.iter().skip(1) {
        assert!(grid
            .kitty_image_store
            .borrow()
            .has_scaled_variant(internal, *key));
    }
    assert!(grid
        .kitty_image_store
        .borrow()
        .has_scaled_variant(internal, extra));

    // Placing columns=2 again refreshes that variant, so the next new size
    // drops columns=3 instead.
    place_scaled(&mut grid, pane_image_id, internal, 1, 2, 1, 0, 0).expect("reuse");
    assert_eq!(
        grid.kitty_image_store.borrow().scale_count(internal),
        MAX_SCALED_VARIANTS_PER_IMAGE as u64 + 1,
        "reusing a stored size must not scale"
    );
    let another =
        place_scaled(&mut grid, pane_image_id, internal, 1, 101, 1, 0, 0).expect("another size");
    assert!(grid
        .kitty_image_store
        .borrow()
        .has_scaled_variant(internal, keys[1]));
    assert!(
        !grid
            .kitty_image_store
            .borrow()
            .has_scaled_variant(internal, keys[2]),
        "the variant that was not reused is dropped"
    );
    assert!(grid
        .kitty_image_store
        .borrow()
        .has_scaled_variant(internal, another));
}

#[test]
fn placement_id_zero_is_a_new_placement_that_reuses_the_scaled_raster() {
    let mut grid = test_grid();
    let (pane_image_id, internal) = transmitted(&mut grid, 1, 1);
    let first = place_scaled(&mut grid, pane_image_id, internal, 0, 4, 2, 0, 0).expect("first");
    let (ptr, _) = scaled_buffer(&grid, internal, first).expect("cached");
    place_scaled(&mut grid, pane_image_id, internal, 0, 4, 2, 0, 0).expect("second");
    assert_eq!(grid.placements().len(), 2);
    assert!(grid
        .placements()
        .iter()
        .all(|placement| placement.placement_id == 0));
    assert_ne!(
        grid.placements()[0].placement_uid,
        grid.placements()[1].placement_uid
    );
    let (again_ptr, _) = scaled_buffer(&grid, internal, first).expect("still cached");
    assert_eq!(again_ptr, ptr);
    assert_eq!(grid.kitty_image_store.borrow().scale_count(internal), 1);
}

#[test]
fn a_cell_size_change_scales_one_shared_destination_once() {
    let cell_size = Rc::new(RefCell::new(Some(SizeInPixels {
        width: 10,
        height: 20,
    })));
    let store = Rc::new(RefCell::new(KittyImageStore::default()));
    let mut grid = KittyGrid::new(cell_size.clone(), store);
    let (pane_image_id, internal) = transmitted(&mut grid, 4, 4);
    let cell = SizeInPixels {
        width: 10,
        height: 20,
    };
    let anchor = KittyVerticalAnchor {
        canonical_line: 0,
        offset_px_from_line_start: 0,
    };
    let command = KittyCommand {
        image_id: Some(pane_image_id),
        columns: 2,
        rows: 2,
        ..Default::default()
    };
    grid.place(pane_image_id, internal, &command, (0, 0), cell, anchor)
        .expect("first");
    grid.place(pane_image_id, internal, &command, (0, 0), cell, anchor)
        .expect("second");
    assert_eq!(grid.placements().len(), 2);
    let before = grid.kitty_image_store.borrow().scale_count(internal);
    *cell_size.borrow_mut() = Some(SizeInPixels {
        width: 20,
        height: 40,
    });
    grid.character_cell_size_possibly_changed();
    assert_eq!(grid.placements().len(), 2);
    assert_eq!(
        grid.kitty_image_store.borrow().scale_count(internal),
        before + 1,
        "two placements of one destination must share one regenerated raster"
    );
}

fn refused_key(width: usize, height: usize) -> super::super::store::ScaledImageKey {
    super::super::store::ScaledImageKey {
        source: PixelRect {
            x: 0,
            y: 0,
            width: 1,
            height: 1,
        },
        size: (width, height),
    }
}

#[test]
fn a_destination_the_store_cannot_retain_is_refused_before_scaling() {
    let mut grid = test_grid();
    let (pane_image_id, internal) = transmitted(&mut grid, 1, 1);
    let retained = grid.kitty_image_store.borrow().total_bytes();
    let max = grid
        .kitty_image_store
        .borrow()
        .max_synchronous_scaled_bytes(internal);
    // 640×320 cells at 8×16 px is 5120×5120 RGBA, about 100 MiB.
    assert!(
        5120 * 5120 * 4 > max,
        "the review destination must be over the retained share ({max} bytes)"
    );
    let scales_before = grid.kitty_image_store.borrow().scale_count(internal);

    for _ in 0..2 {
        let error = place_scaled(&mut grid, pane_image_id, internal, 1, 640, 320, 0, 0)
            .expect_err("a destination the store cannot retain");
        assert_eq!(error.code, KittyErrorCode::Einval);
    }
    assert_eq!(
        grid.kitty_image_store.borrow().scale_count(internal),
        scales_before,
        "the same refused key must not scale again"
    );
    assert!(grid
        .kitty_image_store
        .borrow()
        .scale_was_refused(internal, refused_key(5120, 5120)));
    assert!(grid
        .kitty_image_store
        .borrow()
        .scaled_variant(internal, refused_key(5120, 5120))
        .is_none());
    assert_eq!(grid.kitty_image_store.borrow().total_bytes(), retained);
    assert!(grid.placements().is_empty());

    for columns in 637u32..=640 {
        place_scaled(&mut grid, pane_image_id, internal, 1, columns, 320, 0, 0)
            .expect_err("a cycling destination the store cannot retain");
        place_scaled(&mut grid, pane_image_id, internal, 1, columns, 320, 0, 0)
            .expect_err("the refused key is remembered");
    }
    assert_eq!(
        grid.kitty_image_store.borrow().scale_count(internal),
        scales_before
    );
    assert_eq!(grid.kitty_image_store.borrow().total_bytes(), retained);

    // The refusal did not spend the quota: a destination inside the share still lands.
    let key =
        place_scaled(&mut grid, pane_image_id, internal, 1, 4, 2, 0, 0).expect("a small place");
    assert_eq!(
        scaled_buffer(&grid, internal, key).expect("cached").1,
        32 * 32 * 4
    );
    assert!(grid.kitty_image_store.borrow().total_bytes() > retained);
    assert_eq!(
        grid.kitty_image_store.borrow().scale_count(internal),
        scales_before + 1
    );
}
