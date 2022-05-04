use my_azure_storage_sdk::page_blob::consts::BLOB_PAGE_SIZE;

pub fn get_page_content(src: &[u8], page_id: usize) -> &[u8] {
    let offset = page_id * BLOB_PAGE_SIZE;
    &src[offset..offset + BLOB_PAGE_SIZE]
}

pub fn get_pages_amount(pos: usize, page_size: usize) -> usize {
    if pos == 0 {
        return 0;
    }
    (pos - 1) / page_size + 1
}

pub fn calc_required_pages_amount_after_we_append(
    pos: usize,
    auto_resize_rate_in_pages: usize,
    page_size: usize,
) -> usize {
    if pos == 0 {
        return 0;
    }

    let other_page_size = page_size * auto_resize_rate_in_pages;

    let pages_amount = get_pages_amount(pos, other_page_size);

    pages_amount * auto_resize_rate_in_pages
}

#[cfg(test)]
mod test {

    use super::*;

    #[test]
    fn test_calc_required_pages_amount_after_we_append_one_first_page() {
        assert_eq!(2, calc_required_pages_amount_after_we_append(2, 2, 512));

        assert_eq!(4, calc_required_pages_amount_after_we_append(2, 4, 512));
    }

    #[test]
    fn test_calc_required_pages_amount_after_we_append_one_second_page() {
        assert_eq!(2, calc_required_pages_amount_after_we_append(513, 2, 512));

        assert_eq!(4, calc_required_pages_amount_after_we_append(513, 4, 512));
    }

    #[test]
    fn test_case_3() {
        assert_eq!(2, calc_required_pages_amount_after_we_append(1023, 2, 512));

        assert_eq!(4, calc_required_pages_amount_after_we_append(1023, 4, 512));
    }

    #[test]
    fn test_case_4() {
        assert_eq!(2, calc_required_pages_amount_after_we_append(1024, 2, 512));

        assert_eq!(4, calc_required_pages_amount_after_we_append(1024, 4, 512));
    }

    #[test]
    fn test_case_5() {
        assert_eq!(4, calc_required_pages_amount_after_we_append(1025, 2, 512));

        assert_eq!(4, calc_required_pages_amount_after_we_append(1025, 4, 512));
    }
}
