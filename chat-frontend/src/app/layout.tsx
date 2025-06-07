import type React from "react"
import type { Metadata, Viewport } from "next"
import "./globals.css"

export const metadata: Metadata = {
  title: "Distributed Chat | Raft Consensus",
  description: "A real-time distributed chat application powered by Raft consensus protocol",
  keywords: ["chat", "distributed systems", "raft", "consensus", "real-time"],
  authors: [{ name: "Your Name" }],
  creator: "Your Name",
}

export const viewport: Viewport = {
  themeColor: "#0f172a",
  width: "device-width",
  initialScale: 1,
  maximumScale: 1,
}

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode
}>) {
  return (
    <html lang="en" className="dark">
      <body className="antialiased font-sans">{children}</body>
    </html>
  )
}
