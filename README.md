# Estimating the Number of Movie Torrents

[![Waffle](https://badge.waffle.io/bryantbiggs/movie_torrents.png?label=ready&title=Ready)](https://waffle.io/bryantbiggs/movie_torrents?utm_source=badge)
[![Code Climate](https://codeclimate.com/repos/5990e858ea55a702b0001348/badges/58862f7576d235f05c17/gpa.svg)](https://codeclimate.com/repos/5990e858ea55a702b0001348/feed)
[![Issue Count](https://codeclimate.com/repos/5990e858ea55a702b0001348/badges/58862f7576d235f05c17/issue_count.svg)](https://codeclimate.com/repos/5990e858ea55a702b0001348/feed)
[![Test Coverage](https://codeclimate.com/repos/5990e858ea55a702b0001348/badges/58862f7576d235f05c17/coverage.svg)](https://codeclimate.com/repos/5990e858ea55a702b0001348/coverage)

## Project Description

A multivariate regression model using pre-release film data to estimate the number of [torrent](https://en.wikipedia.org/wiki/Pirated_movie_release_types) copies that might become available online.

![](static/torrents.jpg)

---

## Motivation

As more media content (films, tv shows, music, etc.) move online to streaming providers and other digital sources, 'pirating' content illegally has become easier through the use of torrent sites. This investigation is intended to look at what factors can better predict the number of torrent copies that will become readily available online for a given movie.

---

## Data Sources

- Film Data

  - [The Numbers](http://www.the-numbers.com/movie/budgets/ "The Numbers - Movie Budgets") - The Numbers provides detailed movie financial analysis, including box office, DVD and Blu-ray sales reports, and release schedules
  - [OMDB API](http://www.omdbapi.com/ "The Open Movie Database") - The OMDb API is a free web service to obtain movie information

- Torrent Data

  - [Kickass Torrents](https://kat.cr/ "Kickass Torrent Main Page") - KAT provided a directory for torrent files and magnet links to facilitate peer-to-peer file sharing using the BitTorrent protocol.
  - [Torrentz](https://torrentz.eu/ "Torrentz Main Page") - Torrentz is a meta-search engine for BitTorrent that indexed torrents from various major torrent websites, and offered compilations of various trackers.
  - *Note - The torrent data sources for this project have since been either seized or voluntarily shutdown.*

    - [Kickass Torrents](https://kat.cr/ "Kickass Torrent Main Page") was seized by the [US Justice Department](https://en.wikipedia.org/wiki/KickassTorrents) on July 20th, 2016.
    - [Torrentz](https://torrentz.eu/ "Torrentz Main Page") was voluntarily [shutdown by its owners](https://en.wikipedia.org/wiki/Torrentz) on August 5th, 2016.

---

## Libraries Utilized
  * beautifulsoup4, requests - Retrieve and extract data sources from web
  * s3fs - Data storage in AWS (access available upon request)
  * numpy, pandas - Clean and aggregate data sources
  * sckit-learn - Estimator models

---

## Process
  1. Crawl web pages to retrieve data sources
  2. ETL - Extract data from web page requests, clean up data and transform into respective data type (int, string, etc.), load data into S3 for long term storage
  3. Combine/join data sources into one table/multi-dimensional array
  4. Train and test regression models to make future predictions
  8. Tune/optimize model parameters and perform feature engineering - repeat step 4

---

## Results
- Original [presentation](static/movie_torrents.pdf) delivered on 07/15/2016
