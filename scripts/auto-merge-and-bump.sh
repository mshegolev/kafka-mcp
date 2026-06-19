#!/bin/bash

# Простой скрипт для автоматизации после коммита:
# 1. Пуш изменений
# 2. Проверка статуса CI pipeline (упрощенная)
# 3. Мерж в мастер
# 4. Бамп версии

set -e  # Остановиться при любой ошибке

echo "🚀 Starting automation process..."

# Функция для проверки статуса CI
check_ci_status() {
    echo "⏳ Checking CI pipeline status..."
    # В реальной реализации здесь должна быть интеграция с CI системой
    # Например, для GitHub Actions:
    # gh run list --limit 1 --branch $1 --json status,conclusion --jq '.[].conclusion'
    
    # Для демонстрации просто ждем 30 секунд
    echo "Waiting 30 seconds for CI checks..."
    sleep 30
    echo "✅ Assuming CI passed (in real implementation, this would check actual CI status)"
}

# Функция для бампа версии
bump_version() {
    echo "🔖 Bumping version..."
    
    # Проверяем наличие файла версии
    if [ -f "VERSION" ]; then
        CURRENT_VERSION=$(cat VERSION)
        echo "Current version: $CURRENT_VERSION"
        
        # Парсим версию и увеличиваем минорную часть
        IFS='.' read -ra VERSION_PARTS <<< "$CURRENT_VERSION"
        MAJOR=${VERSION_PARTS[0]}
        MINOR=${VERSION_PARTS[1]}
        PATCH=${VERSION_PARTS[2]}
        
        # Увеличиваем минорную версию и сбрасываем патч
        NEW_MINOR=$((MINOR + 1))
        NEW_VERSION="$MAJOR.$NEW_MINOR.0"
        
        echo "$NEW_VERSION" > VERSION
        echo "New version: $NEW_VERSION"
        return 0
    elif [ -f "pyproject.toml" ]; then
        echo "📝 Updating version in pyproject.toml"
        # В реальной реализации здесь будет код для обновления версии в pyproject.toml
        echo "Note: pyproject.toml version bump logic needs to be implemented"
        return 0
    else
        echo "⚠️  No version file found (VERSION or pyproject.toml)"
        return 1
    fi
}

# Получаем текущую ветку
CURRENT_BRANCH=$(git rev-parse --abbrev-ref HEAD)
echo "Current branch: $CURRENT_BRANCH"

# Пушим изменения
echo "📤 Pushing changes..."
git push origin $CURRENT_BRANCH

# Проверяем CI статус
check_ci_status $CURRENT_BRANCH

# Переходим в мастер и мержим
echo "🔀 Merging to master..."
git checkout master
git pull origin master
git merge $CURRENT_BRANCH --no-ff -m "Merge branch '$CURRENT_BRANCH' into master"

# Пушим мастер
echo "📤 Pushing master..."
git push origin master

# Бампим версию
if bump_version; then
    # Коммитим новую версию
    if [ -f "VERSION" ]; then
        git add VERSION
        git commit -m "version: bump to $(cat VERSION)"
        git push origin master
        echo "✅ Version bumped and committed"
    fi
else
    echo "⚠️  Version bump skipped"
fi

# Возвращаемся к оригинальной ветке
echo "🔄 Returning to original branch..."
git checkout $CURRENT_BRANCH

echo "🎉 Automation process completed!"
echo "Latest master commit: $(git log -1 --oneline master)"
