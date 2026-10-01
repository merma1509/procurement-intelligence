# Astra MIEM Masterskaya - Procurement Intelligence

Recruitment tasks for the Astra workshop (MIEM Moscow Institute of Electronics and Mathematics) focused on applying AI to procurement information collection and processing.

## Project Tracks

### Data Engineer
- Design database schema for construction materials sales data
- Build ETL pipeline with data validation and incremental updates
- Implement error handling and transaction rollback mechanisms
- [Task Details](tasks/data_engineer.md)

### AI Specialist
- Build classification model to predict NHL hockey team success
- Perform exploratory data analysis and feature engineering
- Implement time-series aware train/validation/test split
- [Task Notebook](tasks/ai_specialist.ipynb)

## Setup

### Prerequisites
- Python 3.9+
- [uv](https://github.com/astral-sh/uv) package manager

### Installation

1. Clone the repository:
```bash
git clone https://github.com/merma1509/procurement-intelligence.git
cd procurement-intelligence
```

2. Create virtual environment with uv:
```bash
uv venv procure_venv
```

3. Activate the environment:
```bash
# Windows
procure_venv\Scripts\activate

# Linux/Mac
source procure_venv/bin/activate
```

4. Install dependencies:
```bash
uv sync
```

Dependencies are managed via `pyproject.toml` using uv.

Explore the tasks in the `tasks/` directory.
