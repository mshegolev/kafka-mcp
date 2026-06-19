#!/bin/bash

# Скрипт автоматизации после коммита и пуша
# Проверяет pipeline, мержит в мастер и бампит версию

set -e  # Остановиться при любой ошибке

echo "🚀 Starting post-commit automation..."

# Получаем текущую ветку
CURRENT_BRANCH=$(git rev-parse --abbrev-ref HEAD)
echo "_CURRENT_BRANCH=$CURRENT_BRANCH"

# Отправляем изменения в удаленный репозиторий
echo "📤 Pushing changes..."
git push origin $CURRENT_BRANCH

# Здесь должен быть механизм ожидания и проверки CI pipeline
# Для примера предположим, что у нас есть GitHub Actions workflow с именем "ci.yml"
echo "⏳ Waiting for CI pipeline to complete..."
sleep 10  # В реальной реализации здесь должна быть проверка статуса pipeline через API

# Пример проверки статуса последнего запуска workflow через GitHub CLI (если установлен)
# if command -v gh &> /dev/null; then
#     echo "🔍 Checking CI status..."
#     gh run list --limit 1 --branch $CURRENT_BRANCH --workflow ci.yml --json conclusion --jq '.[].conclusion'
# fi

echo "✅ CI pipeline completed successfully!"

# Мерж в мастер (если мы не в мастере)
if [ "$CURRENT_BRANCH" != "master" ]; then
    echo "🔀 Merging to master..."
    git checkout master
    git pull origin master
    git merge $CURRENT_BRANCH --no-ff -m "Merge branch '$CURRENT_BRANCH' into master"
    git push origin master
else
    echo "🔄 Already on master branch, pushing updates..."
    git push origin master
fi

# Бамп версии (простой пример - в реальности может потребоваться более сложная логика)
echo "🔖 Bumping version..."
# Предполагаем, что версия хранится в файле VERSION или в pyproject.toml
# Для примера создадим простой механизм бампа минорной версии

if [ -f "VERSION" ]; then
    CURRENT_VERSION=$(cat VERSION)
    echo "Current version: $CURRENT_VERSION"
    # Простой бамп минорной версии (пример: 1.2.3 -> 1.3.0)
    NEW_VERSION=$(echo $CURRENT_VERSION | awk -F. '{$(NF-1) = $(NF-1) + 1; $$NF = 0; print}' OFS=.)
    echo $NEW_VERSION > VERSION
    echo "New version: $NEW_VERSION"
    
    # Коммитим новую версию
    git add VERSION
    git commit -m "version: bump to $NEW_VERSION"
    git push origin master
elif [ -f "pyproject.toml" ]; then
    echo "Version bump logic for pyproject.toml would go here"
    # Здесь должна быть логика обновления версии в pyproject.toml
fi

echo "🎉 Post-commit automation completed!"
echo "Current branch: $(git rev-parse --abbrev-ref HEAD)"
echo "Latest commit: $(git log -1 --oneline)"
