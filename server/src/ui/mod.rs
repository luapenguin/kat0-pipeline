// SPDX-License-Identifier: AGPL-3.0-only

pub mod browser;
pub mod terminal;

use std::fmt;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum UiMode {
    #[default]
    Terminal,
    Browser,
    Both,
}

impl UiMode {
    pub fn parse(value: &str) -> Result<Self, String> {
        match value {
            "terminal" => Ok(Self::Terminal),
            "browser" => Ok(Self::Browser),
            "both" => Ok(Self::Both),
            other => Err(format!(
                "Modo --ui no válido: {other:?} (usa terminal, browser o both)"
            )),
        }
    }

    pub fn uses_terminal(self) -> bool {
        matches!(self, Self::Terminal | Self::Both)
    }

    pub fn uses_browser(self) -> bool {
        matches!(self, Self::Browser | Self::Both)
    }
}

impl fmt::Display for UiMode {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Terminal => "terminal",
            Self::Browser => "browser",
            Self::Both => "both",
        })
    }
}
