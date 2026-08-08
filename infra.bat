@echo off
setlocal

set "SCRIPT_DIR=%~dp0"
set "COMPOSE_FILE=%SCRIPT_DIR%Infra\docker-compose-infra.yml"
set "ENV_FILE=%SCRIPT_DIR%Infra\infra.env"
set "ACTION=%~1"
set "DRY_RUN="
@REM set "NETWORKS=infra-net infra-api infra-observability infra-persistance"
set "NETWORKS=infra-net"

if /I "%~2"=="--dry-run" set "DRY_RUN=1"

if not exist "%COMPOSE_FILE%" (
    echo Compose file not found: "%COMPOSE_FILE%"
    exit /b 1
)

if not exist "%ENV_FILE%" (
    echo Env file not found: "%ENV_FILE%"
    exit /b 1
)

if /I "%ACTION%"=="start" (
    set "COMMAND=docker compose --env-file "%ENV_FILE%" -f "%COMPOSE_FILE%" up --build -d"
) else if /I "%ACTION%"=="stop" (
    set "COMMAND=docker compose --env-file "%ENV_FILE%" -f "%COMPOSE_FILE%" down"
) else (
    echo Usage: %~nx0 ^<start^|stop^> [--dry-run]
    exit /b 1
)

if defined DRY_RUN (
    if /I "%ACTION%"=="start" (
        for %%N in (%NETWORKS%) do echo [network] ensure %%N ^(create if missing^)
    ) else (
        for %%N in (%NETWORKS%) do echo [network] remove %%N
    )
    echo %COMMAND%
    exit /b 0
)

pushd "%SCRIPT_DIR%" >nul
if /I "%ACTION%"=="start" (
    for %%N in (%NETWORKS%) do (
        echo [network] checking %%N
        docker network inspect %%N >nul 2>&1
        if errorlevel 1 (
            echo [network] creating %%N
            docker network create %%N >nul
        ) else (
            echo [network] exists %%N
        )
    )
    docker compose --env-file "%ENV_FILE%" -f "%COMPOSE_FILE%" up --build -d
) else (
    docker compose --env-file "%ENV_FILE%" -f "%COMPOSE_FILE%" down
    set "EXIT_CODE=%ERRORLEVEL%"
    for %%N in (%NETWORKS%) do (
        echo [network] removing %%N
        docker network rm %%N >nul 2>&1
    )
    popd >nul
    exit /b %EXIT_CODE%
)
set "EXIT_CODE=%ERRORLEVEL%"
popd >nul

exit /b %EXIT_CODE%