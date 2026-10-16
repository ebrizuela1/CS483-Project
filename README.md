# CS483 Project

## Problem and Goal

We want to build a model that predicts stock prices for trading. The goal is a model that exceeds standard benchmark measures such as Sharpe ratio or percentage return.

### Research Question

Can a model predict stock prices or signals that produce positive returns from publicly available data?

## Data Plan

### Necessary Data

- **Text.** Yahoo Finance news and company reports
- **Numerical.** Stock prices over time (time series)

### How

Data comes from public datasets or is scraped from relevant websites.

### Where

| Source | Contents | Time Estimate |
| --- | --- | --- |
| FNSPID ([GitHub](https://github.com/Zdong104/FNSPID_Financial_News_Dataset), [Hugging Face](https://huggingface.co/datasets/Zihan1004/FNSPID), [arXiv](https://arxiv.org/abs/2402.06698)) | S&P 500 prices and news, 1999 to 2023 | Hours |
| Kaggle dataset | Supplementary news and prices | Hours |
| [sec.gov EDGAR](https://www.sec.gov/Archives/edgar/full-index/) | Filings, earnings releases, and footnotes | 1 to 2 days |

## Data Mining Task

### Methods

- NLP features, TF-IDF on news and filings
- MinHash and LSH duplicate handling
- Spark MLlib to rank stocks weekly

### Goals

- Design a model that produces stock signals and measure its performance by Sharpe ratio
- Design a dashboard or terminal that visualizes sentiment, signals, and performance

### Expected Results

- Public data will give a small edge that is canceled out by trading costs
- Adding text sentiment to price data will be key to beating a price only model

## Schedule

| Week | Milestone |
| --- | --- |
| W8 Friday (10/16) | Start |
| W10 | Ensure data quality, collect data, test implementation and experimentation |
| W11 Sunday | Midterm check |
| W13 | Polish repo, show methodology, ensure implementation quality |
| W14 | Slides, presentation prep and practice, final polishing |
| W15 Sunday | Final due |

## Team Process

- **Model.** Kanban (Agile) with one task board (To Do, Doing, Done)
- **Board.** GitHub Projects, linked to this repo
- **Check in.** Once a week
- **Code.** Work on branches and merge to `main` through pull requests
