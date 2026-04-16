use clap::{Parser, Subcommand};

/// Anthropic <-> Kiro API 客户端
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
pub struct Args {
    /// 配置文件路径
    #[arg(short, long, global = true)]
    pub config: Option<String>,

    /// 凭证文件路径
    #[arg(long, global = true)]
    pub credentials: Option<String>,

    #[command(subcommand)]
    pub command: Option<Command>,
}

#[derive(Subcommand, Debug)]
pub enum Command {
    /// 从 external backend 导出 file backend 回滚文件
    ExportFileState(ExportFileStateArgs),
}

#[derive(clap::Args, Debug, Clone)]
pub struct ExportFileStateArgs {
    /// 导出目录；会生成 credentials.json、config.rollback.json 等回滚文件
    #[arg(long, default_value = "rollback-export")]
    pub output_dir: String,

    /// 允许覆盖已存在的导出文件
    #[arg(long, default_value_t = false)]
    pub overwrite: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[test]
    fn parse_default_serve_mode_without_subcommand() {
        let args = Args::parse_from(["kiro-rs", "--config", "config.json"]);

        assert_eq!(args.config.as_deref(), Some("config.json"));
        assert!(args.command.is_none());
    }

    #[test]
    fn parse_export_file_state_subcommand() {
        let args = Args::parse_from([
            "kiro-rs",
            "--config",
            "config.json",
            "export-file-state",
            "--output-dir",
            "/tmp/rollback",
            "--overwrite",
        ]);

        match args.command {
            Some(Command::ExportFileState(command)) => {
                assert_eq!(command.output_dir, "/tmp/rollback");
                assert!(command.overwrite);
            }
            other => panic!("unexpected command: {other:?}"),
        }
    }

    #[test]
    fn parse_global_flags_after_subcommand() {
        let args = Args::parse_from([
            "kiro-rs",
            "export-file-state",
            "--output-dir",
            "/tmp/rollback",
            "--config",
            "config.json",
            "--credentials",
            "credentials.json",
        ]);

        assert_eq!(args.config.as_deref(), Some("config.json"));
        assert_eq!(args.credentials.as_deref(), Some("credentials.json"));
    }
}
