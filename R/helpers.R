#' Fix ECB package to use new API endpoint
#' @export
fix_ecb_url <- function() {

  create_query_url_fixed <- function(key, filter = NULL) {
    # New ECB API endpoint
    url <- "https://data-api.ecb.europa.eu/service/data"

    flow_ref <- regmatches(key, regexpr("^[[:alnum:]]+", key))
    key_q <- regmatches(key, regexpr("^[[:alnum:]]+\\.", key),
                        invert = TRUE)[[1]][2]

    if (any(names(filter) == "")) {
      stop("All filter parameters must be named!")
    }

    if ("updatedAfter" %in% names(filter)) {
      filter$updatedAfter <- curl::curl_escape(filter$updatedAfter)
    }

    # Build query string
    if (!is.null(filter) && length(filter) > 0) {
      names_esc <- curl::curl_escape(names(filter))
      values_esc <- curl::curl_escape(as.character(filter))
      query <- paste0(names_esc, "=", values_esc, collapse = "&")
      query <- paste0("?", query)
    } else {
      query <- ""
    }

    # Construct URL - NO trailing slash
    query_url <- paste0(url, "/", flow_ref, "/", key_q, query)
    query_url
  }

  # Patch the package namespace
  assignInNamespace("create_query_url", create_query_url_fixed, ns = "ecb")

  message("ECB package patched to use new API endpoint: data-api.ecb.europa.eu")
  invisible(NULL)
}


#' Extract key dimensions from ECB series key
#'
#' Filters get_dimensions() output to only include actual key dimensions,
#' excluding attributes.
#'
#' @param key Character, ECB series key
#' @param dims_result Output from ecb::get_dimensions(key)
#'
#' @return Data frame with only key dimensions (dim, value columns)
#' @keywords internal
extract_key_dimensions <- function(key, dims_result) {

  # Count dimension values in key (parts after dataflow code)
  key_parts <- stringr::str_split(key, "\\.")[[1]]
  dataflow <- key_parts[1]
  dimension_values <- key_parts[-1]  # Everything after dataflow
  n_dims <- length(dimension_values)

  # First n rows are the key dimensions, rest are attributes
  key_dims <- dims_result[1:n_dims, ]

  # Verify we got the right ones by checking values match
  if (!all(key_dims$value == dimension_values)) {
    warning("Dimension value mismatch - dimension order may be incorrect")
  }

  key_dims
}


#' Construct ECB series code from series key
#'
#' Converts an ECB series key (e.g., "ICP.M.U2.N.000000.4.ANR") to the
#' database series code format (e.g., "ECB--ICP--U2--N--000000--4--ANR--M").
#'
#' @param series_key Character. An ECB series key.
#'
#' @return Character. The constructed series code.
#' @export
construct_series_code <- function(series_key) {
  # Parse series key
  dims_list <- ecb::get_dimensions(series_key)
  key_dims <- extract_key_dimensions(series_key, dims_list[[1]])

  dataflow_code <- regmatches(series_key, regexpr("^[[:alnum:]]+", series_key))

  # Extract interval (FREQ dimension)
  interval_id <- key_dims$value[key_dims$dim == "FREQ"]

  # Get dimension values excluding FREQ
  non_freq_dims <- key_dims[key_dims$dim != "FREQ", ]

  # Construct series_code: source--dataflow--dimension_values--interval
  paste0(
    "ECB--",
    dataflow_code, "--",
    paste(non_freq_dims$value, collapse = "--"), "--",
    interval_id
  )
}


#' Format period ID for database insertion
#'
#' Converts ECB period formats to UMAR database period format by removing or
#' replacing separators based on the interval type.
#'
#' @param period Character. The period string from ECB (e.g., "2024", "2024-Q2", "2024-02").
#' @param interval_id Character. The interval type: "A" (annual), "Q" (quarterly),
#'   or "M" (monthly).
#'
#' @return Character. The formatted period ID for database insertion.
#'
#' @details
#' Formatting rules:
#' \itemize{
#'   \item Annual (A): "2024" → "2024" (no change)
#'   \item Quarterly (Q): "2024-Q2" → "2024Q2" (remove hyphen)
#'   \item Monthly (M): "2024-02" → "2024M02" (replace hyphen with "M")
#' }
#'
#' @examples
#' format_period_id("2024", "A")        # "2024"
#' format_period_id("2024-Q2", "Q")    # "2024Q2"
#' format_period_id("2024-02", "M")    # "2024M02"
#'
#' @export
format_period_id <- function(period, interval_id) {
  switch(interval_id,
         "A" = period,                           # Annual: no change
         "Q" = gsub("-", "", period),            # Quarterly: remove hyphen
         "M" = gsub("-", "M", period),           # Monthly: replace hyphen with M
         stop("Unknown interval_id: ", interval_id, ". Expected 'A', 'Q', or 'M'.")
  )
}
