import type { Metadata } from "next";
import { Inter } from "next/font/google";
import "./globals.css";

const inter = Inter({ subsets: ["latin"], variable: '--font-geist-sans' });

export const metadata: Metadata = {
  title: "Distributed Chat Application",
  description: "A real-time chat application using Raft consensus",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en">
      <body className={`${inter.className} h-screen`}>{children}</body>
    </html>
  );
}