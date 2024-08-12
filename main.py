from fastapi import FastAPI, HTTPException, Depends, Query
from pydantic import BaseModel
from typing import List, Optional, Literal
from datetime import datetime, timezone, date

from github_parser import get_top100_from_db, get_repo_activity


app = FastAPI()

# Определяем модель данных для эндпоинта /api/repos/top100
class Repo(BaseModel):
    repo: str
    owner: str
    position_cur: int
    position_prev: Optional[int]
    stars: int
    watchers: int
    forks: int
    open_issues: int
    language: Optional[str]


# Определяем модель данных для эндпоинта /api/repos/{owner}/{repo}/activity
class ActivityDay(BaseModel):
    date: date
    commits: int
    authors: List[str]


def validate_dates(
        since: Optional[datetime] = Query(None, description="Start date in ISO 8601 format"), 
        until: Optional[datetime] = Query(None, description="End date in ISO 8601 format")
):
    """
    Обрабатывает возможные проблемы со значениями since, until и выбрасывает ошибки с подсказками для пользователя
    """
    
    if since is None or until is None:
        raise HTTPException(status_code=400, detail="Please provide both 'since' and 'until' parameters in ISO 8601 format.")
    
    # Преобразовываем даты к временной зоне UTC (если не указана иная)
    # Это позволит избежать ошибки для такого кейса since = 2024-08-03, until = 2024-08-09T20:00:00Z
    if since.tzinfo is None:
        since = since.replace(tzinfo=timezone.utc)
    if until.tzinfo is None:
        until = until.replace(tzinfo=timezone.utc)

    if until <= since:
        raise HTTPException(status_code=400, detail="'until' must be greater than 'since'")
    return since, until


@app.get("/api/repos/top100", response_model=List[Repo])
def top_100_repos(sort: str = 'position_cur', order: Literal['asc', 'desc'] = 'asc'):
    rows = get_top100_from_db()
    
    if not rows:
        raise HTTPException(status_code=404, detail="No repos found")
    
    repos = [
        Repo(
            repo=row[0],
            owner=row[1],
            position_cur=row[2],
            position_prev=row[3],
            stars=row[4],
            watchers=row[5],
            forks=row[6],
            open_issues=row[7],
            language=row[8]
        )
        for row in rows
    ]
    
    repos.sort(key=lambda item: getattr(item, sort), reverse=(order == 'desc'))
    
    return repos


@app.get("/api/repos/{owner}/{repo}/activity", response_model=List[ActivityDay])
def show_repo_activity(owner: str, repo: str, dates: tuple[datetime, datetime] = Depends(validate_dates)):
    
    since, until = dates
    activity_data = get_repo_activity(owner, repo, since, until)

    days = [
        ActivityDay(
            date=key,
            commits=activity_data[key]['commits'],
            authors=activity_data[key]['authors']
        )
        for key in activity_data
    ]
    
    days.sort(key=lambda item: item.date, reverse=True)

    return days