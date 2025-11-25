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
