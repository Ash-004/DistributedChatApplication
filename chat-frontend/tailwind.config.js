/** @type {import('tailwindcss').Config} */
module.exports = {
  content: [
    './src/pages/**/*.{js,ts,jsx,tsx,mdx}',
    './src/components/**/*.{js,ts,jsx,tsx,mdx}',
    './src/app/**/*.{js,ts,jsx,tsx,mdx}',
  ],
  theme: {
    extend: {
      colors: {
        foreground: 'rgb(var(--foreground-rgb))',
        background: {
          primary: 'rgb(var(--background-primary))',
          secondary: 'rgb(var(--background-secondary))'
        },
        accent: {
          teal: 'rgb(var(--accent-teal))',
          purple: 'rgb(var(--accent-purple))'
        },
        text: {
          secondary: 'rgb(var(--text-secondary))'
        }
      },
      fontFamily: {
        sans: ['Inter', '-apple-system', 'BlinkMacSystemFont', 'Segoe UI', 'Roboto', 'sans-serif']
      },
      keyframes: {
        'fade-in': {
          from: {
            opacity: '0',
            transform: 'translateY(20px)'
          },
          to: {
            opacity: '1',
            transform: 'translateY(0)'
          }
        },
        'fade-in-up': {
          from: {
            opacity: '0',
            transform: 'translateY(40px)'
          },
          to: {
            opacity: '1',
            transform: 'translateY(0)'
          }
        },
        'scale-in': {
          from: {
            opacity: '0',
            transform: 'scale(0.9)'
          },
          to: {
            opacity: '1',
            transform: 'scale(1)'
          }
        },
        'slide-in': {
          from: {
            opacity: '0',
            transform: 'translateX(-30px)'
          },
          to: {
            opacity: '1',
            transform: 'translateX(0)'
          }
        },
        'bounce-in': {
          '0%': {
            opacity: '0',
            transform: 'translateY(20px) scale(0.9)'
          },
          '50%': {
            opacity: '1',
            transform: 'translateY(-5px) scale(1.05)'
          },
          '100%': {
            opacity: '1',
            transform: 'translateY(0) scale(1)'
          }
        },
        'pulse-glow': {
          '0%, 100%': {
            boxShadow: '0 0 20px rgba(6, 182, 212, 0.3), 0 0 40px rgba(6, 182, 212, 0.15)'
          },
          '50%': {
            boxShadow: '0 0 30px rgba(6, 182, 212, 0.45), 0 0 60px rgba(6, 182, 212, 0.25)'
          }
        },
        'gradient-x': {
          '0%, 100%': {
            transform: 'translateX(0%)'
          },
          '50%': {
            transform: 'translateX(100%)'
          }
        },
        'blob': {
          '0%': {
            transform: 'translate(0px, 0px) scale(1)'
          },
          '33%': {
            transform: 'translate(30px, -50px) scale(1.1)'
          },
          '66%': {
            transform: 'translate(-20px, 20px) scale(0.9)'
          },
          '100%': {
            transform: 'translate(0px, 0px) scale(1)'
          }
        },
        'float': {
          '0%, 100%': {
            transform: 'translateY(0px)'
          },
          '50%': {
            transform: 'translateY(-10px)'
          }
        },
        'float-delayed': {
          '0%, 100%': {
            transform: 'translateY(0px)'
          },
          '50%': {
            transform: 'translateY(-15px)'
          }
        }
      },
      animation: {
        'fade-in': 'fade-in 0.5s ease-out',
        'fade-in-up': 'fade-in-up 0.5s ease-out',
        'scale-in': 'scale-in 0.3s ease-out',
        'slide-in': 'slide-in 0.3s ease-out',
        'bounce-in': 'bounce-in 0.5s cubic-bezier(0.68, -0.55, 0.265, 1.55)',
        'pulse-glow': 'pulse-glow 2s ease-in-out infinite',
        'gradient-x': 'gradient-x 15s ease infinite',
        'blob': 'blob 7s infinite',
        'float': 'float 6s ease-in-out infinite',
        'float-delayed': 'float-delayed 8s ease-in-out infinite'
      }
    }
  },
  plugins: []
}
