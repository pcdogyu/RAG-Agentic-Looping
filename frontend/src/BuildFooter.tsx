const commitId = __BUILD_COMMIT_ID__;

export const buildInfo = {
  branch: __BUILD_BRANCH__,
  commitId,
  commitTime: __BUILD_COMMIT_TIME__,
};

export default function BuildFooter() {
  return (
    <footer className="build-footer" aria-label="构建版本信息">
      <span>
        Code by <a href="mailto:Yuhao@jiansutech.co">Yuhao@jiansutech.co</a>
      </span>
      <span aria-hidden="true">-</span>
      <time dateTime={buildInfo.commitTime}>{buildInfo.commitTime}</time>
      <span aria-hidden="true">-</span>
      <span title={buildInfo.commitId}>{commitId === "unknown" ? commitId : commitId.slice(0, 12)}</span>
      <span aria-hidden="true">-</span>
      <span>{buildInfo.branch}</span>
    </footer>
  );
}
