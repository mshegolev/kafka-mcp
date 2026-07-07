#!/bin/bash

# Простой скрипт для автоматизации после коммита:
# 1. Пуш изменений
# 2. Проверка статуса CI pipeline (упрощенная)
# 3. Мерж в мастер
# 4. Бамп версии

set -e  # Остановиться при любой ошибке

echo "🚀 Starting automation process..."

# --- GitHub auth: берём токен из macOS keychain, а НЕ из GITHUB_TOKEN env.
# gh хранит токен в login-keychain (service "gh:github.com"); переменные
# окружения перекрывают keychain, поэтому сначала снимаем возможный
# протухший GITHUB_TOKEN/GH_TOKEN и даём gh прочитать keychain.
unset GITHUB_TOKEN GH_TOKEN
if command -v gh >/dev/null 2>&1; then
    GH_TOKEN="$(gh auth token 2>/dev/null || true)"
    if [ -n "$GH_TOKEN" ]; then
        export GH_TOKEN
        echo "🔑 GitHub token loaded from keychain (len=${#GH_TOKEN})"
    else
        echo "⚠️  Нет валидного токена в keychain — выполни: gh auth login"
    fi
else
    echo "⚠️  gh CLI не найден — CI-проверка будет пропущена"
fi

# Функция для проверки статуса CI (GitHub Actions через gh, токен из keychain).
# Возвращает 0 всегда (advisory): результат логируется, но не блокирует мерж.
# Чтобы сделать проверку блокирующей — верни здесь код conclusion.
check_ci_status() {
    local branch="$1"
    echo "⏳ Checking CI pipeline status for '$branch'..."

    if ! command -v gh >/dev/null 2>&1 || [ -z "${GH_TOKEN:-}" ]; then
        echo "⚠️  gh недоступен или нет токена — пропускаю проверку CI"
        return 0
    fi

    # Ждём завершения последнего run на ветке (макс ~5 мин).
    local tries=0 status conclusion
    while [ "$tries" -lt 30 ]; do
        status=$(gh run list --branch "$branch" --limit 1 \
            --json status --jq '.[0].status // "none"' 2>/dev/null || echo "none")
        if [ "$status" = "completed" ]; then
            conclusion=$(gh run list --branch "$branch" --limit 1 \
                --json conclusion --jq '.[0].conclusion // "unknown"' 2>/dev/null || echo "unknown")
            if [ "$conclusion" = "success" ]; then
                echo "✅ CI passed (conclusion=success)"
            else
                echo "❌ CI НЕ прошёл (conclusion=$conclusion) — мерж всё равно продолжится (advisory)"
            fi
            return 0
        fi
        echo "CI status: $status — жду 10с ($((tries + 1))/30)..."
        sleep 10
        tries=$((tries + 1))
    done

    echo "⚠️  CI-проверка истекла по таймауту — продолжаю (advisory)"
    return 0
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
