import os
import subprocess
from pathlib import Path

def find_docker_compose_files(directory):
    """Находит файлы docker-compose.yaml только в указанной директории (без подпапок)."""
    compose_files = []
    # Проверяем только текущую директорию (не рекурсивно)
    target_path = Path(directory)
    if (target_path / "docker-compose.yaml").exists():
        compose_files.append(target_path / "docker-compose.yaml")
    return compose_files

def run_services(compose_files):
    """Сначала собирает, затем запускает все сервисы из docker-compose файлов."""
    for file in compose_files:
        print(f"🔨 Сборка сервисов из файла: {file}")
        try:
            # Сначала выполняем build
            subprocess.run(
                ["docker-compose", "-f", str(file), "build"],
                check=True,
            )
            print(f"🚀 Запуск сервисов из файла: {file}")
            # Затем up
            subprocess.run(
                ["docker-compose", "-f", str(file), "up", "-d"],
                check=True,
            )
        except subprocess.CalledProcessError as e:
            print(f"❌ Ошибка при обработке {file}: {e}")
        except FileNotFoundError:
            print("❌ Убедитесь, что Docker и docker-compose установлены!")

if __name__ == "__main__":
    DIRs = ['.', "DAGs/LTV", 'budibase/hosting']
    for DIR in DIRs:
        compose_files = find_docker_compose_files(DIR)
        if not compose_files:
            print(f"⚠️ В папке {DIR} не найдено docker-compose.yaml файлов.")
        else:
            print(f"🔍 Найдены файлы: {[str(f) for f in compose_files]} в папке {DIR}")
            run_services(compose_files)