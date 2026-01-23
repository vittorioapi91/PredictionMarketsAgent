pipeline {
    agent any
    
    // Pipeline for PredictionMarketsAgent - Polymarket data collection
    // This pipeline validates and tests the Polymarket data collection scripts
    
    environment {
        PYTHON_VERSION = '3.8'
    }
    
    stages {
        stage('Checkout') {
            steps {
                checkout scm
                script {
                    env.GIT_COMMIT = sh(
                        script: 'git rev-parse HEAD',
                        returnStdout: true
                    ).trim()
                    env.GIT_COMMIT_SHORT = sh(
                        script: 'git rev-parse --short HEAD',
                        returnStdout: true
                    ).trim()
                    env.GIT_BRANCH = sh(
                        script: 'git rev-parse --abbrev-ref HEAD',
                        returnStdout: true
                    ).trim()
                    env.GIT_URL = sh(
                        script: 'git config --get remote.origin.url',
                        returnStdout: true
                    ).trim()
                    
                    echo "Building for branch: ${env.GIT_BRANCH}"
                    echo "Commit: ${env.GIT_COMMIT_SHORT}"
                }
            }
        }
        
        stage('Validate Branch Naming') {
            steps {
                script {
                    def branch = env.GIT_BRANCH
                    echo "Validating branch name: ${branch}"
                    
                    // Allowed branches: main, staging
                    if (branch == 'main' || branch == 'staging') {
                        echo "✓ Branch '${branch}' is allowed (main/staging)"
                        return
                    }
                    
                    // Dev branches must follow pattern: dev/PMA-{issue}/{package}
                    def devPattern = ~/^dev\/PMA-(\d+)\/([a-zA-Z0-9_-]+)$/
                    def matcher = branch =~ devPattern
                    
                    if (!matcher.matches()) {
                        error("""
❌ Invalid branch name: ${branch}

Branch naming rules:
  - Allowed branches: 'main', 'staging'
  - Dev branches must follow: dev/PMA-{openproject-issue}/{package-name}
  - Example: dev/PMA-37/polymarket

Your branch does not match any allowed pattern.
                        """.stripIndent())
                    }
                    
                    // Extract issue number and package name
                    def issueNumber = matcher[0][1]
                    def packageName = matcher[0][2]
                    
                    echo "Branch pattern matched:"
                    echo "  Issue: PMA-${issueNumber}"
                    echo "  Package: ${packageName}"
                    
                    // Validate OpenProject issue exists
                    echo "Validating OpenProject issue PMA-${issueNumber}..."
                    
                    // Get OpenProject configuration from environment variables
                    def openProjectUrl = env.OPENPROJECT_URL ?: 'http://openproject.local.info'
                    def openProjectUser = env.OPENPROJECT_USER ?: ''
                    def openProjectPassword = env.OPENPROJECT_PASSWORD ?: ''
                    
                    if (!openProjectUser || !openProjectPassword) {
                        echo "⚠️  Warning: OPENPROJECT_USER or OPENPROJECT_PASSWORD not set in Jenkins"
                        echo "  Skipping OpenProject validation for this build."
                        echo "  Branch pattern is valid: dev/PMA-${issueNumber}/${packageName}"
                        echo "  Please verify issue PMA-${issueNumber} exists manually."
                        return
                    }
                    
                    // Use basic auth to check if work package exists
                    def auth = "${openProjectUser}:${openProjectPassword}".bytes.encodeBase64().toString()
                    
                    def response = sh(
                        script: """
                            curl -s -w "\\n%{http_code}" \\
                                -H "Authorization: Basic ${auth}" \\
                                -H "Content-Type: application/json" \\
                                "${openProjectUrl}/api/v3/work_packages/${issueNumber}" || echo "\\n000"
                        """,
                        returnStdout: true
                    ).trim()
                    
                    def lines = response.split('\n')
                    def httpCode = lines[-1]
                    def responseBody = lines[0..-2].join('\n')
                    
                    if (httpCode == '200') {
                        // Success - issue exists
                        try {
                            def json = readJSON text: responseBody
                            def workPackageId = json.id?.toString()
                            def workPackageSubject = json.subject ?: 'N/A'
                            echo "✓ OpenProject issue PMA-${issueNumber} exists"
                            echo "  ID: ${workPackageId}"
                            echo "  Subject: ${workPackageSubject}"
                        } catch (Exception e) {
                            // If we can't parse JSON but got 200, assume it's valid
                            echo "✓ OpenProject issue PMA-${issueNumber} exists (could not parse response details)"
                            echo "  Response: ${responseBody.take(200)}"
                        }
                    } else if (httpCode == '404') {
                        // Issue not found
                        error("""
❌ OpenProject issue PMA-${issueNumber} not found

The branch name references an OpenProject issue that does not exist.
Please verify the issue number or create the issue in OpenProject.
                        """.stripIndent())
                    } else if (httpCode == '401' || httpCode == '403') {
                        // Authentication/authorization error
                        echo "⚠️  Warning: Could not authenticate with OpenProject (HTTP ${httpCode})"
                        echo "  This may be due to:"
                        echo "    1. Incorrect credentials"
                        echo "    2. API authentication method changed"
                        echo "    3. Server configuration issue"
                        echo ""
                        echo "  Skipping OpenProject validation for this build."
                        echo "  Branch pattern is valid: dev/PMA-${issueNumber}/${packageName}"
                        echo "  Please verify issue PMA-${issueNumber} exists manually."
                    } else {
                        // Other error (network, server error, etc.)
                        error("""
❌ Failed to validate OpenProject issue PMA-${issueNumber}

HTTP Status: ${httpCode}
Response: ${responseBody.take(500)}

Could not connect to OpenProject server or validate the issue.
Please check:
  1. OpenProject server is accessible at ${openProjectUrl}
  2. Network connectivity
  3. Server is responding correctly
                        """.stripIndent())
                    }
                    
                    echo "✓ Branch naming validation passed"
                }
            }
        }
        
        stage('Validate Project Structure') {
            steps {
                script {
                    echo "Validating project structure..."
                    sh """
                        # Check required directories exist
                        if [ ! -d "src" ]; then
                            echo "❌ ERROR: src/ directory not found"
                            exit 1
                        fi
                        
                        if [ ! -d "tests" ]; then
                            echo "⚠️  Warning: tests/ directory not found (tests are optional)"
                        fi
                        
                        # Check required source files exist
                        if [ ! -f "src/main.py" ]; then
                            echo "❌ ERROR: src/main.py not found"
                            exit 1
                        fi
                        
                        # Check for polymarket package
                        if [ ! -d "src/polymarket" ]; then
                            echo "❌ ERROR: src/polymarket/ directory not found"
                            exit 1
                        fi
                        
                        if [ ! -f "src/polymarket/client.py" ]; then
                            echo "❌ ERROR: src/polymarket/client.py not found"
                            exit 1
                        fi
                        
                        if [ ! -f "src/polymarket/market_processor.py" ]; then
                            echo "❌ ERROR: src/polymarket/market_processor.py not found"
                            exit 1
                        fi
                        
                        if [ ! -f "src/polymarket/data_pipeline.py" ]; then
                            echo "❌ ERROR: src/polymarket/data_pipeline.py not found"
                            exit 1
                        fi
                        
                        if [ ! -f "requirements.txt" ]; then
                            echo "❌ ERROR: requirements.txt not found"
                            exit 1
                        fi
                        
                        echo "✓ Project structure is valid"
                        echo "Source files:"
                        ls -la src/*.py 2>/dev/null || true
                        echo "Polymarket package files:"
                        ls -la src/polymarket/*.py 2>/dev/null || true
                    """
                }
            }
        }
        
        stage('Validate Python Syntax') {
            steps {
                script {
                    echo "Validating Python syntax..."
                    sh """
                        # Check Python version
                        python3 --version
                        
                        # Validate syntax of all Python files
                        echo "Checking Python syntax in src/..."
                        for file in src/*.py; do
                            if [ -f "\$file" ]; then
                                echo "Validating \$file..."
                                python3 -m py_compile "\$file" || {
                                    echo "❌ Syntax error in \$file"
                                    exit 1
                                }
                            fi
                        done
                        
                        # Validate syntax in polymarket package
                        if [ -d "src/polymarket" ]; then
                            echo "Checking Python syntax in src/polymarket/..."
                            for file in src/polymarket/*.py; do
                                if [ -f "\$file" ]; then
                                    echo "Validating \$file..."
                                    python3 -m py_compile "\$file" || {
                                        echo "❌ Syntax error in \$file"
                                        exit 1
                                    }
                                fi
                            done
                        fi
                        
                        # Validate syntax in tests/ if it exists
                        if [ -d "tests" ]; then
                            echo "Checking Python syntax in tests/..."
                            for file in tests/*.py; do
                                if [ -f "\$file" ]; then
                                    echo "Validating \$file..."
                                    python3 -m py_compile "\$file" || {
                                        echo "❌ Syntax error in \$file"
                                        exit 1
                                    }
                                fi
                            done
                        fi
                        
                        echo "✓ All Python files have valid syntax"
                    """
                }
            }
        }
        
        stage('Validate Dependencies') {
            steps {
                script {
                    echo "Validating dependencies..."
                    sh """
                        # Check if requirements.txt is valid
                        if [ ! -f "requirements.txt" ]; then
                            echo "❌ ERROR: requirements.txt not found"
                            exit 1
                        fi
                        
                        # Validate requirements.txt format (basic check)
                        echo "Checking requirements.txt format..."
                        while IFS= read -r line || [ -n "\$line" ]; do
                            # Skip empty lines
                            if [ -z "\$line" ]; then
                                continue
                            fi
                            # Skip lines that are just whitespace
                            trimmed=\$(echo "\$line" | xargs)
                            if [ -z "\$trimmed" ]; then
                                continue
                            fi
                            # Skip comment lines
                            if echo "\$line" | grep -qE '^[[:space:]]*#'; then
                                continue
                            fi
                            # Check if line looks like a valid requirement
                            if ! echo "\$line" | grep -qE '^[a-zA-Z0-9_-]+[>=<]?[0-9.]+'; then
                                echo "⚠️  Warning: Line may not be a valid requirement: \$line"
                            fi
                        done < requirements.txt
                        
                        echo "✓ Dependencies file validated"
                    """
                }
            }
        }
        
        stage('Install Dependencies') {
            steps {
                script {
                    echo "Installing dependencies..."
                    sh """
                        # Create virtual environment if it doesn't exist
                        if [ ! -d "venv" ]; then
                            python3 -m venv venv
                        fi
                        
                        # Use virtual environment's Python directly
                        VENV_PYTHON="venv/bin/python"
                        VENV_PIP="venv/bin/pip"
                        
                        # Upgrade pip first
                        \${VENV_PIP} install --quiet --upgrade pip
                        
                        # Install project dependencies
                        echo "Installing dependencies from requirements.txt..."
                        \${VENV_PIP} install --quiet -r requirements.txt || {
                            echo "❌ ERROR: Failed to install dependencies"
                            exit 1
                        }
                        
                        echo "✓ Dependencies installed successfully"
                        echo "Installed packages:"
                        \${VENV_PIP} list | grep -E '(pandas|python-dotenv|requests)' || true
                    """
                }
            }
        }
        
        stage('Validate Imports') {
            steps {
                script {
                    echo "Validating Python imports..."
                    sh """
                        VENV_PYTHON="venv/bin/python"
                        
                        # Test imports for each source file
                        echo "Testing imports in src/main.py..."
                        \${VENV_PYTHON} -c "
import sys
sys.path.insert(0, 'src')
try:
    import main
    print('✓ main imports successfully')
except ImportError as e:
    print(f'❌ Import error in main: {e}')
    sys.exit(1)
except Exception as e:
    print(f'⚠️  Warning in main: {e}')
" || exit 1
                        
                        echo "Testing imports from src.polymarket package..."
                        \${VENV_PYTHON} -c "
import sys
sys.path.insert(0, '.')
try:
    from src.polymarket import PolymarketClient, MarketDataProcessor, DatabaseManager
    print('✓ polymarket package imports successfully')
except ImportError as e:
    print(f'❌ Import error in polymarket package: {e}')
    sys.exit(1)
except Exception as e:
    print(f'⚠️  Warning in polymarket package: {e}')
" || exit 1
                        
                        echo "Testing imports in src/polymarket/data_pipeline.py..."
                        \${VENV_PYTHON} -c "
import sys
sys.path.insert(0, '.')
try:
    from src.polymarket.data_pipeline import DataPipeline
    print('✓ data_pipeline imports successfully')
except ImportError as e:
    print(f'❌ Import error in data_pipeline: {e}')
    sys.exit(1)
except Exception as e:
    print(f'⚠️  Warning in data_pipeline: {e}')
" || exit 1
                        
                        echo "✓ All imports validated successfully"
                    """
                }
            }
        }
        
        stage('Run Tests') {
            when {
                // Only run tests if tests directory exists and has Python files
                expression {
                    fileExists('tests') && sh(
                        script: 'find tests -name "*.py" -type f | head -1',
                        returnStdout: true
                    ).trim()
                }
            }
            steps {
                script {
                    echo "Running unit tests..."
                    sh """
                        VENV_PYTHON="venv/bin/python"
                        VENV_PIP="venv/bin/pip"
                        
                        # Install pytest if not already installed
                        \${VENV_PIP} install --quiet pytest pytest-cov || {
                            echo "⚠️  Warning: Could not install pytest. Skipping tests."
                            exit 0
                        }
                        
                        # Create test results directory
                        mkdir -p test-results
                        
                        # Run tests with coverage
                        set +e  # Don't exit on error immediately
                        \${VENV_PYTHON} -m pytest tests/ \
                            --junitxml=test-results/junit.xml \
                            --cov=src \
                            --cov-report=html:test-results/coverage \
                            --cov-report=xml:test-results/coverage.xml \
                            -v || {
                            echo "⚠️  Some tests failed. Check output above for details."
                            TEST_EXIT_CODE=\$?
                        }
                        set -e  # Re-enable exit on error
                        
                        # Check if test results were generated
                        if [ -f "test-results/junit.xml" ]; then
                            echo "✓ Test results generated: test-results/junit.xml"
                        else
                            echo "⚠️  Warning: JUnit XML file was not generated"
                        fi
                        
                        # Show test summary
                        if [ -f "test-results/junit.xml" ]; then
                            echo "Test Summary:"
                            \${VENV_PYTHON} -c "
import xml.etree.ElementTree as ET
try:
    tree = ET.parse('test-results/junit.xml')
    root = tree.getroot()
    tests = int(root.attrib.get('tests', 0))
    failures = int(root.attrib.get('failures', 0))
    errors = int(root.attrib.get('errors', 0))
    print(f'  Tests: {tests}, Failures: {failures}, Errors: {errors}')
except Exception as e:
    print(f'  Could not parse test results: {e}')
" || true
                        fi
                    """
                }
            }
            post {
                always {
                    // Archive test results (JUnit XML)
                    script {
                        try {
                            if (fileExists('test-results/junit.xml')) {
                                junit 'test-results/junit.xml'
                            }
                        } catch (Exception e) {
                            echo "Warning: Could not archive JUnit test results: ${e.message}"
                        }
                        
                        // Publish coverage report if it exists
                        try {
                            if (fileExists('test-results/coverage.xml')) {
                                publishCoverage adapters: [
                                    coberturaAdapter('test-results/coverage.xml')
                                ], sourceFileResolver: sourceFiles('STORE_LAST_BUILD')
                            }
                        } catch (Exception e) {
                            echo "Warning: Could not publish coverage report: ${e.message}"
                        }
                    }
                }
            }
        }
        
        stage('Validate Script Logic') {
            steps {
                script {
                    echo "Validating script logic (dry-run checks)..."
                    sh """
                        VENV_PYTHON="venv/bin/python"
                        
                        # Check that main functions exist and are callable
                        echo "Checking script entry points..."
                        
                        # Check main.py
                        \${VENV_PYTHON} -c "
import sys
sys.path.insert(0, 'src')
import main
if hasattr(main, 'main'):
    print('✓ main has main() function')
else:
    print('⚠️  Warning: main does not have main() function')
" || true
                        
                        # Check data_pipeline.py
                        \${VENV_PYTHON} -c "
import sys
sys.path.insert(0, '.')
from src.polymarket.data_pipeline import DataPipeline
if hasattr(DataPipeline, 'run'):
    print('✓ DataPipeline has run() method')
else:
    print('⚠️  Warning: DataPipeline does not have run() method')
" || true
                        
                        echo "✓ Script logic validation completed"
                    """
                }
            }
        }
    }
    
    post {
        success {
            echo "✓ PredictionMarketsAgent pipeline succeeded!"
            
            // Post success status to GitHub
            script {
                try {
                    // Extract repo owner/name from git URL
                    def repoUrl = env.GIT_URL.replace('.git', '').replace('git@github.com:', 'https://github.com/').replace('https://github.com/', '')
                    def repoParts = repoUrl.split('/')
                    def repoOwner = repoParts.size() >= 1 ? repoParts[0] : 'vittorioapi91'
                    def repoName = repoParts.size() >= 2 ? repoParts[1] : 'PredictionMarketsAgent'
                    
                    // Get GitHub token from credentials or environment
                    def githubToken = env.GITHUB_TOKEN ?: ''
                    
                    if (githubToken) {
                        // Post status using curl
                        sh """
                            curl -X POST \\
                              -H "Authorization: token ${githubToken}" \\
                              -H "Accept: application/vnd.github.v3+json" \\
                              "https://api.github.com/repos/${repoOwner}/${repoName}/statuses/${env.GIT_COMMIT}" \\
                              -d '{
                                "state": "success",
                                "target_url": "${env.BUILD_URL}",
                                "description": "Jenkins pipeline passed",
                                "context": "jenkins/pipeline"
                              }' || echo "Warning: Could not post status to GitHub"
                        """
                    } else {
                        echo "Info: GITHUB_TOKEN not set. Skipping GitHub status update."
                    }
                } catch (Exception e) {
                    echo "Warning: Could not post status to GitHub: ${e.message}"
                }
            }
        }
        failure {
            echo "✗ PredictionMarketsAgent pipeline failed. Check logs for details."
            
            // Post failure status to GitHub
            script {
                try {
                    def repoUrl = env.GIT_URL.replace('.git', '').replace('git@github.com:', 'https://github.com/').replace('https://github.com/', '')
                    def repoParts = repoUrl.split('/')
                    def repoOwner = repoParts.size() >= 1 ? repoParts[0] : 'vittorioapi91'
                    def repoName = repoParts.size() >= 2 ? repoParts[1] : 'PredictionMarketsAgent'
                    
                    def githubToken = env.GITHUB_TOKEN ?: ''
                    
                    if (githubToken) {
                        sh """
                            curl -X POST \\
                              -H "Authorization: token ${githubToken}" \\
                              -H "Accept: application/vnd.github.v3+json" \\
                              "https://api.github.com/repos/${repoOwner}/${repoName}/statuses/${env.GIT_COMMIT}" \\
                              -d '{
                                "state": "failure",
                                "target_url": "${env.BUILD_URL}",
                                "description": "Jenkins pipeline failed",
                                "context": "jenkins/pipeline"
                              }' || echo "Warning: Could not post status to GitHub"
                        """
                    }
                } catch (Exception e) {
                    echo "Warning: Could not post status to GitHub: ${e.message}"
                }
            }
        }
        always {
            // Clean up virtual environment (optional - comment out if you want to keep it)
            // Uncomment the following to clean up venv after each build
            // sh "rm -rf venv" || true
            echo "Build completed"
        }
    }
}
