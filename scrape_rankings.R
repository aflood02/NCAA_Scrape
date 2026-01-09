library(dplyr)
library(tidyr)
library(readr)
library(purrr)
library(rvest)
library(httr)
library(jsonlite)
library(chromote)
library(lubridate)

# Create data directory if it doesn't exist
if (!dir.exists("data")) {
  dir.create("data")
}

# ==========================================
# HELPER FUNCTIONS
# ==========================================

normalize_bpi_name <- function(team_name) {
  cleaned <- gsub("'", "", team_name)
  cleaned <- iconv(cleaned, to = "ASCII//TRANSLIT")
  cleaned <- gsub("\\bUniversity of\\b", "", cleaned, ignore.case = TRUE)
  cleaned <- gsub("\\bU\\.?\\s*of\\b", "", cleaned, ignore.case = TRUE)
  cleaned <- gsub("\\bSt\\.\\s+", "Saint ", cleaned)
  cleaned <- gsub("\\s+", " ", cleaned)
  cleaned <- trimws(cleaned)
  
  bpi_mappings <- c(
    "Miami \\(FL\\)" = "Miami FL",
    "Miami FL" = "Miami FL",
    "Miami \\(OH\\)" = "Miami OH",
    "Miami OH" = "Miami OH",
    "USC" = "Southern California",
    "UConn" = "Connecticut",
    "SMU" = "Southern Methodist",
    "TCU" = "Texas Christian",
    "VCU" = "Virginia Commonwealth",
    "UCF" = "Central Florida",
    "LSU" = "Louisiana St",
    "BYU" = "Brigham Young",
    "UNLV" = "Nevada Las Vegas"
  )
  
  for (pattern in names(bpi_mappings)) {
    cleaned <- gsub(paste0("^", pattern, "$"), bpi_mappings[pattern], cleaned, ignore.case = TRUE)
  }
  
  return(cleaned)
}

strip_mascot_name <- function(team_name) {
  mascots <- c(
    "Screaming Eagles", "Golden Griffins", "Purple Eagles", "Delta Devils",
    "Black Knights", "Golden Lions", "Runnin Bulldogs", "Fighting Camels",
    "Rainbow Warriors", "Fighting Illini", "Fighting Irish", "Fighting Hawks",
    "Golden Hurricane", "Demon Deacons", "Horned Frogs", "Blue Demons",
    "Blue Devils", "Blue Hens", "Blue Hose", "Blue Raiders", "Crimson Tide",
    "Golden Bears", "Golden Eagles", "Golden Flashes", "Golden Gophers",
    "Golden Grizzlies", "Great Danes", "Green Wave", "Mean Green", "Big Green",
    "Mountain Hawks", "Nittany Lions", "Purple Aces", "Ragin Cajuns",
    "Red Foxes", "River Hawks", "Scarlet Knights", "Sun Devils", "Tar Heels",
    "Thundering Herd", "Yellow Jackets", "Red Storm", "Red Raiders", "Red Wolves",
    "Red Flash", "Running Bulldogs", "Wolf Pack", "Big Red", "Black Bears",
    "Aggies", "Anteaters", "Aztecs", "Badgers", "Beach", "Beacons", "Bearkats",
    "Bears", "Bearcats", "Beavers", "Bengals", "Billikens", "Bison", "Bisons",
    "Black", "Blackbirds", "Blazers", "Bluejays", "Bobcats", "Boilermakers",
    "Bonnies", "Braves", "Broncos", "Bruins", "Buccaneers", "Buckeyes",
    "Buffaloes", "Bulldogs", "Bulls", "Camels", "Cardinal", "Cardinals",
    "Catamounts", "Cavaliers", "Chanticleers", "Chargers", "Chippewas", "Clan",
    "Colonels", "Colonials", "Commodores", "Cornhuskers", "Cougars", "Cowboys",
    "Coyotes", "Crimson", "Crusaders", "Cyclones", "Deacons", "Demons",
    "Dolphins", "Dons", "Dragons", "Ducks", "Dukes", "Eagles", "Explorers",
    "Falcons", "Fighting", "Flames", "Flyers", "Friars", "Gaels", "Gamecocks",
    "Gators", "Gauchos", "Golden", "Gophers", "Governors", "Greyhounds",
    "Grizzlies", "Hatters", "Hawks", "Hawkeyes", "Highlanders", "Hilltoppers",
    "Hokies", "Hornets", "Hoosiers", "Hoyas", "Huskies", "Hurricanes", "Illini",
    "Islanders", "Jaguars", "Jackrabbits", "Jaspers", "Jayhawks", "Jets",
    "Johnnies", "Knights", "Lakers", "Lancers", "Leathernecks", "Leopards",
    "Lions", "Lobos", "Longhorns", "Lopes", "Lumberjacks", "Mastodons",
    "Matadors", "Mavericks", "Midshipmen", "Miners", "Minutemen", "Mocs",
    "Monarchs", "Mountain", "Mountaineers", "Musketeers", "Mustangs", "Norse",
    "Orangemen", "Orange", "Ospreys", "Owls", "Paladins", "Panthers", "Patriots",
    "Peacocks", "Penguins", "Phoenix", "Pilots", "Pirates", "Pioneers", "Pokes",
    "Pride", "Privateers", "Purple", "Pythons", "Quakers", "Racers", "Raiders",
    "Rainbow", "Rams", "Ramblers", "Rattlers", "Ravens", "Razorbacks", "Rebels",
    "Redhawks", "Redbirds", "Retrievers", "Revolutionaries", "River", "Riverhawks",
    "Roadrunners", "Rockets", "Roos", "Royals", "Saints", "Salukis", "Scots",
    "Seahawks", "Seawolves", "Seminoles", "Sharks", "Shockers", "Skyhawks",
    "Sooners", "Spartans", "Spiders", "Stags", "Sycamores", "Terrapins",
    "Terriers", "Texans", "Thunderbirds", "Tigers", "Titans", "Tommies",
    "Toreros", "Trailblazers", "Tribe", "Tritons", "Trojans", "Utes", "Vandals",
    "Vaqueros", "Vikings", "Violets", "Volunteers", "Vulcans", "Warhawks",
    "Warriors", "Waves", "Wildcats", "Wolves", "Wolfpack", "Wolverines",
    "Wonders", "Zips", "49ers"
  )
  
  mascots <- mascots[order(-nchar(mascots))]
  pattern <- paste0("\\s+(", paste(mascots, collapse = "|"), ")$")
  cleaned <- gsub(pattern, "", team_name, ignore.case = TRUE)
  
  return(trimws(cleaned))
}

# ==========================================
# SCRAPING FUNCTIONS
# ==========================================

scrape_bpi <- function() {
  message("Scraping BPI rankings...")
  tryCatch({
    b <- ChromoteSession$new()
    b$Page$navigate("https://www.espn.com/mens-college-basketball/bpi")
    b$Page$loadEventFired()
    Sys.sleep(3)
    
    # Click "Show More" multiple times
    for (i in 1:15) {
      tryCatch({
        show_more_result <- b$Runtime$evaluate('
          var button = document.querySelector("a.AnchorLink.loadMore__link");
          if (button && button.offsetParent !== null) {
            button.click();
            true;
          } else {
            false;
          }
        ')
        
        if (show_more_result$result$value) {
          message("Clicked Show More button (", i, "/15)")
          Sys.sleep(1.5)
        } else {
          message("No more data to load (clicked ", i-1, " times)")
          break
        }
      }, error = function(e) {
        message("Finished clicking Show More after ", i-1, " attempts")
        break
      })
    }
    
    html_content <- b$Runtime$evaluate('document.documentElement.outerHTML')
    page <- read_html(html_content$result$value)
    b$close()
    
    tables <- page %>%
      html_nodes("table") %>%
      html_table(fill = TRUE)
    
    if (length(tables) == 0) {
      message("No BPI tables found")
      return(NULL)
    }
    
    bpi_table <- tables[[1]]
    
    if (ncol(bpi_table) >= 2) {
      bpi_data <- data.frame(
        team_raw = as.character(bpi_table[[1]]),
        bpi_raw = as.character(bpi_table[[2]]),
        stringsAsFactors = FALSE
      )
      
      bpi_data$rank_raw <- 1:nrow(bpi_data)
    } else {
      return(NULL)
    }
    
    bpi_data <- bpi_data %>%
      mutate(
        team = team_raw,
        team = ifelse(grepl("Miami.*\\(OH\\)", team, ignore.case = TRUE), "Miami OH", team),
        team = ifelse(grepl("Miami.*\\(FL\\)", team, ignore.case = TRUE), "Miami FL", team),
        team = gsub("\\s*\\(.*?\\)", "", team),
        team = normalize_bpi_name(team),
        team = strip_mascot_name(team),
        team = gsub("\\s*\\d+$", "", team),
        team = trimws(team),
        bpi_rank = rank_raw,
        bpi_rating = as.numeric(gsub("[^0-9.-]", "", bpi_raw))
      ) %>%
      filter(
        !is.na(bpi_rank),
        bpi_rank > 0,
        team != "",
        !is.na(team),
        team != "Team",
        team != "TEAM",
        nchar(team) > 1
      ) %>%
      select(team, bpi_rank, bpi_rating) %>%
      group_by(team) %>%
      slice(1) %>%
      ungroup()
    
    message("Successfully scraped BPI: ", nrow(bpi_data), " teams")
    return(bpi_data)
    
  }, error = function(e) {
    message("Error scraping BPI: ", e$message)
    return(NULL)
  })
}

scrape_kpi <- function() {
  message("Scraping KPI rankings...")
  tryCatch({
    b <- ChromoteSession$new()
    b$Page$navigate("https://faktorsports.com/#/home")
    b$Page$loadEventFired()
    
    message("Waiting for KPI table to load...")
    
    max_attempts <- 20
    for (attempt in 1:max_attempts) {
      Sys.sleep(1)
      
      row_check <- b$Runtime$evaluate('
        var tables = document.querySelectorAll("table");
        var maxRows = 0;
        for (var i = 0; i < tables.length; i++) {
          var rows = tables[i].querySelectorAll("tr").length;
          if (rows > maxRows) maxRows = rows;
        }
        maxRows;
      ')
      
      max_rows <- row_check$result$value
      message("Attempt ", attempt, "/", max_attempts, ": Found max ", max_rows, " rows")
      
      if (max_rows > 100) {
        message("KPI table loaded!")
        break
      }
    }
    
    Sys.sleep(2)
    
    html_content <- b$Runtime$evaluate('document.documentElement.outerHTML')
    page <- read_html(html_content$result$value)
    b$close()
    
    tables <- page %>%
      html_nodes("table") %>%
      html_table(fill = TRUE)
    
    if (length(tables) > 0) {
      table_sizes <- sapply(tables, nrow)
      largest_idx <- which.max(table_sizes)
      kpi_table <- tables[[largest_idx]]
      
      if (nrow(kpi_table) > 10) {
        kpi_data <- data.frame(
          team_raw = as.character(kpi_table[[1]]),
          kpi_value_raw = as.character(kpi_table[[4]]),
          stringsAsFactors = FALSE
        ) %>%
          mutate(
            kpi_rank = row_number(),
            team = gsub("\\s*\\(.*?\\)", "", team_raw),
            team = trimws(team),
            kpi_value = suppressWarnings(as.numeric(kpi_value_raw))
          ) %>%
          filter(
            team != "",
            !is.na(team),
            team != "Team",
            team != "KPI",
            nchar(team) > 1
          ) %>%
          select(team, kpi_rank, kpi_value) %>%
          group_by(team) %>%
          slice(1) %>%
          ungroup()
        
        if (nrow(kpi_data) > 100) {
          message("Successfully scraped KPI: ", nrow(kpi_data), " teams")
          return(kpi_data)
        }
      }
    }
    
    message("Failed to extract KPI data")
    return(NULL)
    
  }, error = function(e) {
    message("Error scraping KPI: ", e$message)
    return(NULL)
  })
}

scrape_barttorvik <- function() {
  message("Scraping BartTorvik rankings...")
  tryCatch({
    b <- ChromoteSession$new()
    b$Page$navigate("https://barttorvik.com/")
    b$Page$loadEventFired()
    
    message("Waiting for BartTorvik table...")
    Sys.sleep(5)
    
    html_content <- b$Runtime$evaluate('document.documentElement.outerHTML')
    page <- read_html(html_content$result$value)
    b$close()
    
    tables <- page %>%
      html_nodes("table") %>%
      html_table(fill = TRUE)
    
    if (length(tables) == 0) {
      message("No tables found")
      return(NULL)
    }
    
    torvik_table <- tables[[1]]
    
    if (ncol(torvik_table) < 8) {
      message("Table has insufficient columns")
      return(NULL)
    }
    
    # Determine column indices - WAB is typically around column 10-11
    # You may need to adjust this based on the actual table structure
    wab_col_idx <- if(ncol(torvik_table) >= 10) 10 else NA
    
    torvik_data <- data.frame(
      rank_raw = as.character(torvik_table[[1]]),
      team_raw = as.character(torvik_table[[2]]),
      barthag_raw = as.character(torvik_table[[8]]),
      adjoe_raw = as.character(torvik_table[[6]]),
      adjde_raw = as.character(torvik_table[[7]]),
      wab_raw = if(!is.na(wab_col_idx)) as.character(torvik_table[[wab_col_idx]]) else NA_character_,
      stringsAsFactors = FALSE
    )
    
    torvik_data <- torvik_data %>%
      mutate(
        torvik_rank = as.integer(gsub("[^0-9]", "", rank_raw)),
        team = team_raw,
        # Remove game corruption
        team = gsub("\\s+\\([HAN]\\)\\s+\\d+.*$", "", team),
        team = gsub("\\s*\\([HAN]\\).*$", "", team),
        team = gsub("\\s+\\(won\\)\\s*$", "", team),
        team = gsub("\\s+\\(lost\\)\\s*$", "", team),
        team = gsub("\\s*\\([^)]*\\)\\s*$", "", team),
        team = gsub("\\s*\\d+$", "", team),
        team = trimws(team),
        # Apply standard normalization
        team = normalize_bpi_name(team),
        team = strip_mascot_name(team),
        # Fix State/Saint issues
        team = gsub("\\s+Saint$", " State", team),
        team = gsub("Cal Saint", "Cal State", team, fixed = TRUE),
        # Extract numeric values
        torvik_rating = suppressWarnings(as.numeric(barthag_raw)),
        torvik_adjoe = suppressWarnings(as.numeric(adjoe_raw)),
        torvik_adjde = suppressWarnings(as.numeric(adjde_raw)),
        torvik_wab = suppressWarnings(as.numeric(wab_raw))
      ) %>%
      filter(
        !is.na(torvik_rank),
        torvik_rank > 0,
        team != "",
        !is.na(team),
        !team %in% c("Team", "TEAM", "Rk", "D-I Avg:"),
        nchar(team) > 1,
        nchar(team) < 50
      ) %>%
      arrange(desc(torvik_wab)) %>%
      mutate(torvik_wab_rank = row_number()) %>%  # Create WAB rank
      arrange(torvik_rank) %>%  # Restore original rank order
      select(team, torvik_rank, torvik_rating, torvik_adjoe, torvik_adjde, torvik_wab, torvik_wab_rank) %>%
      group_by(team) %>%
      slice(1) %>%
      ungroup()
    
    message("Successfully scraped BartTorvik: ", nrow(torvik_data), " teams")
    message("WAB data captured: ", sum(!is.na(torvik_data$torvik_wab)), " teams with WAB values")
    return(torvik_data)
    
  }, error = function(e) {
    message("Error scraping BartTorvik: ", e$message)
    return(NULL)
  })
}
# ==========================================
# MAIN EXECUTION
# ==========================================

message("Starting scraping job at ", Sys.time())

# Scrape BPI
bpi_data <- scrape_bpi()
if (!is.null(bpi_data)) {
  write_csv(bpi_data, "data/bpi_rankings.csv")
  message("Saved BPI data: ", nrow(bpi_data), " teams")
}

# Scrape KPI
kpi_data <- scrape_kpi()
if (!is.null(kpi_data)) {
  write_csv(kpi_data, "data/kpi_rankings.csv")
  message("Saved KPI data: ", nrow(kpi_data), " teams")
}

# Scrape BartTorvik
torvik_data <- scrape_barttorvik()
if (!is.null(torvik_data)) {
  write_csv(torvik_data, "data/barttorvik_rankings.csv")
  message("Saved BartTorvik data: ", nrow(torvik_data), " teams")
}

# Create metadata file with timestamp
metadata <- data.frame(
  last_updated = Sys.time(),
  bpi_teams = ifelse(!is.null(bpi_data), nrow(bpi_data), 0),
  kpi_teams = ifelse(!is.null(kpi_data), nrow(kpi_data), 0),
  barttorvik_teams = ifelse(!is.null(torvik_data), nrow(torvik_data), 0)
)
write_csv(metadata, "data/metadata.csv")

message("Scraping job completed at ", Sys.time())
