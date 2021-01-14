import logging

logger = logging.getLogger(__name__)


def update_import(stage, rev=None):
    stage.deps[0].update(rev=rev)
    frozen = stage.frozen
    stage.frozen = False
    try:
        stage.reproduce()
    finally:
        stage.frozen = frozen


def sync_import(stage, dry=False, force=False, jobs=None):
    """Synchronize import's outs to the workspace."""
    assert len(stage.deps) == 1
    assert len(stage.outs) == 1
    [dep] = stage.deps
    [out] = stage.outs

    logger.info("Importing '{dep}' -> '{out}'".format(dep=dep, out=out))
    if dry:
        return

    if not force and stage.already_cached():
        out.checkout()
    else:
        stage.save_deps(dry=True)
        out.hash_info = dep.download(out, jobs=jobs)
