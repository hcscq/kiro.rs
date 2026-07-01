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
    /// 探测上游真实配额边界，输出 JSONL 观测记录
    QuotaProbe(QuotaProbeArgs),
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

#[derive(clap::Args, Debug, Clone)]
pub struct QuotaProbeArgs {
    /// 探测模式：fixed、ramp-rpm 或 ramp-tpm
    #[arg(long, default_value = "fixed")]
    pub mode: String,

    /// 目标模型
    #[arg(long, default_value = "claude-sonnet-4.5")]
    pub model: String,

    /// 仅探测指定凭据 ID；缺失时按过滤条件选择全部未禁用凭据
    #[arg(long)]
    pub credential_id: Option<u64>,

    /// 仅探测指定认证账号类型，如 enterprise/social/builder-id/idc
    #[arg(long)]
    pub auth_account_type: Option<String>,

    /// 仅探测指定账号类型，如 power/pro-plus/free
    #[arg(long)]
    pub account_type: Option<String>,

    /// 每个目标 RPM 下发送多少个请求
    #[arg(long, default_value_t = 10)]
    pub requests_per_step: u32,

    /// fixed 模式目标 RPM；ramp-rpm 模式起始 RPM
    #[arg(long, default_value_t = 6.0)]
    pub rpm: f64,

    /// ramp-rpm 模式结束 RPM
    #[arg(long)]
    pub max_rpm: Option<f64>,

    /// ramp-rpm 模式每步增加的 RPM
    #[arg(long, default_value_t = 6.0)]
    pub rpm_step: f64,

    /// ramp-tpm 模式起始 TPM；通过固定 RPM、增加每请求输入 token 近似实现
    #[arg(long, default_value_t = 6000.0)]
    pub tpm: f64,

    /// ramp-tpm 模式结束 TPM
    #[arg(long)]
    pub max_tpm: Option<f64>,

    /// ramp-tpm 模式每步增加的 TPM
    #[arg(long, default_value_t = 6000.0)]
    pub tpm_step: f64,

    /// 并发请求数；建议先从 1 开始
    #[arg(long, default_value_t = 1)]
    pub concurrency: usize,

    /// 构造 prompt 的目标输入 token 数（近似）
    #[arg(long, default_value_t = 256)]
    pub input_tokens: u32,

    /// 请求 max_tokens
    #[arg(long, default_value_t = 64)]
    pub max_tokens: i32,

    /// 请求超时时间（秒）
    #[arg(long, default_value_t = 180)]
    pub timeout_seconds: u64,

    /// JSONL 输出路径；缺失时输出到 stdout
    #[arg(long)]
    pub output: Option<String>,

    /// 错误 body 摘要最大字符数
    #[arg(long, default_value_t = 512)]
    pub error_excerpt_chars: usize,

    /// 遇到 429 后是否停止当前凭据当前阶梯
    #[arg(long, default_value_t = false)]
    pub stop_on_429: bool,

    /// 只打印将要探测的凭据和阶梯，不发送上游请求
    #[arg(long, default_value_t = false)]
    pub dry_run: bool,
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
    fn parse_quota_probe_subcommand() {
        let args = Args::parse_from([
            "kiro-rs",
            "--config",
            "config.json",
            "quota-probe",
            "--mode",
            "ramp-rpm",
            "--auth-account-type",
            "enterprise",
            "--rpm",
            "12",
            "--max-rpm",
            "60",
        ]);

        match args.command {
            Some(Command::QuotaProbe(command)) => {
                assert_eq!(command.mode, "ramp-rpm");
                assert_eq!(command.auth_account_type.as_deref(), Some("enterprise"));
                assert_eq!(command.rpm, 12.0);
                assert_eq!(command.max_rpm, Some(60.0));
            }
            other => panic!("unexpected command: {other:?}"),
        }
    }

    #[test]
    fn parse_quota_probe_tpm_subcommand() {
        let args = Args::parse_from([
            "kiro-rs",
            "quota-probe",
            "--mode",
            "ramp-tpm",
            "--rpm",
            "6",
            "--tpm",
            "6000",
            "--max-tpm",
            "18000",
            "--tpm-step",
            "6000",
        ]);

        match args.command {
            Some(Command::QuotaProbe(command)) => {
                assert_eq!(command.mode, "ramp-tpm");
                assert_eq!(command.rpm, 6.0);
                assert_eq!(command.tpm, 6000.0);
                assert_eq!(command.max_tpm, Some(18000.0));
                assert_eq!(command.tpm_step, 6000.0);
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
