"""
После запуска docker-compose.yaml
Нужно убедиться что budibase инициализирован, т.е создан пользователь и хотя бы один app в нём
Иначе бекап не будет выполняться

Запуск: python backup.py
При запуске делает бекап данных budibase в директории /backups
и копирует файл бекапа с именем budibase_latest.tar.gz

При следующем запуске или при переносе при запуске контейнер bb-importer автоматически импортирует
данные из бекапа в budibase из файла backups/budibase_latest.tar.gz
"""

import argparse, datetime as dt, shutil, subprocess, sys
from pathlib import Path

DEFAULT_BASE = Path.cwd()
CLI_TAG      = "budibase-cli:local"
DOCKERFILE   = Path(__file__).with_name("Dockerfile_budibase_cli")
CONTAINER_NAME = "bbimporter"
NETWORK      = "bi_network"

def run(cmd: list[str], quiet=False, **kw) -> None:
    """
    Запуск команды
    """
    if not quiet:
        print("+", " ".join(cmd))
    subprocess.run(cmd, check=True, **kw)

def is_running(name: str) -> bool:
    """
    Проверка запущен ли контейнер (CONTAINER_NAME)
    """
    try:
        state = subprocess.check_output(
            ["docker", "inspect", "-f", "{{.State.Running}}", name],
            stderr=subprocess.DEVNULL, text=True).strip()
        return state.lower() == "true"
    except subprocess.CalledProcessError:
        return False

def ensure_cli(dockerfile: Path, quiet=False) -> None:
    """
    Запуск контейнера в консоли
    """
    try:
        run(["docker", "image", "inspect", CLI_TAG],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, quiet=True)
    except subprocess.CalledProcessError:
        if not dockerfile.exists():
            sys.exit("[ERROR] Dockerfile_budibase_cli not found")
        run(["docker", "build", "-t", CLI_TAG,
             "-f", str(dockerfile), str(dockerfile.parent)], quiet=quiet)

def read_env(path: Path) -> dict[str, str]:
    """
    Чтение .env
    """
    env = {}
    for line in path.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            env[k] = v
    return env


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("-b", "--base", type=Path, default=DEFAULT_BASE,
                    help="каталог с .env и backups/")
    args = ap.parse_args()

    base      = args.base.expanduser().resolve()
    env_file  = base / ".env"
    backups   = base / "backups"
    backups.mkdir(exist_ok=True)

    if not env_file.exists():
        sys.exit(f"[ERROR] .env not found: {env_file}")

    ts       = dt.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    archive  = backups / f"budibase_{ts}.tar.gz"
    latest   = backups / "budibase_latest.tar.gz"

    env = read_env(env_file)
    couch_user = env.get("COUCH_DB_USER", "budibase")
    couch_pass = env.get("COUCH_DB_PASSWORD", "budibase")
    env["COUCH_DB_URL"] = f"http://{couch_user}:{couch_pass}@couchdb-service:5984"
    env["MINIO_URL"]    = "http://minio-service:9000"

    env["CI"] = "true"
    # построим список "-e K=V"
    env_flags = sum([["-e", f"{k}={v}"] for k, v in env.items()], [])

    # bbimporter запущен
    if is_running(CONTAINER_NAME):
        run([
            "docker", "exec", CONTAINER_NAME,
            "budi", "backups",
            "--export", f"/backups/{archive.name}", "--env", "/config/.env",
        ])

    # иначе поднимаем
    else:
        ensure_cli(DOCKERFILE)
        run([
            "docker", "run", "--rm",
            "--network", NETWORK,
            "-v", f"{backups}:/backups",
            *env_flags,
            CLI_TAG,
            "backups",
            "--export", f"/backups/{archive.name}"
        ])

    # копирование в budibase_latest.tar.gz
    try:
        shutil.copy2(archive, latest)
        print(f"Backup saved   : {archive}")
        print(f"Latest copied  : {latest}")
    except OSError as exc:
        print(f"[WARN] Could not copy latest: {exc}", file=sys.stderr)

if __name__ == "__main__":
    main()
