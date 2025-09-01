from logging.config import fileConfig
from sqlalchemy import engine_from_config, pool
from alembic import context

from classes.database_models import BaseApp  # Your ORM models

# Alembic Config object
config = context.config

# Set up Python logging
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# Target metadata for 'autogenerate'
target_metadata = BaseApp.metadata


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode."""
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode with batch mode for SQLite."""
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        # Enable batch mode for SQLite
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            render_as_batch=True,  # critical for SQLite ALTER TABLE
        )

        with context.begin_transaction():
            context.run_migrations()


# Determine offline/online mode
if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
