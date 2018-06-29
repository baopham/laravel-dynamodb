#!/usr/bin/env bash

composer global require squizlabs/php_codesniffer slevomat/coding-standard sirbrillig/phpcs-variable-analysis
COMPOSER_HOME=$(composer config home)
export PATH=$PATH:$COMPOSER_HOME/vendor/bin
phpcs --config-set ignore_warnings_on_exit 1
phpcs --config-set installed_paths $COMPOSER_HOME/vendor/slevomat/coding-standard,$COMPOSER_HOME/vendor/sirbrillig/phpcs-variable-analysis