/** @odoo-module **/
import { patch } from "@web/core/utils/patch";
import { NavBar } from "@web/webclient/navbar/navbar";
import { useRef, onMounted, onWillUnmount, useState } from "@odoo/owl";
import { localization } from "@web/core/l10n/localization";

patch(NavBar.prototype, {
    setup(){
        super.setup();
        this.horizontalAppBar = useRef("horizontalAppBar");
        this.horizontalBarContent = useRef("horizontalBarContent");
        this.handleAppClick = this.handleAppClick.bind(this);
        this.detectViewport = this.detectViewport.bind(this);
        
        // Add reactive state for viewport type
        this.viewportState = useState({
            isMobile: window.innerWidth <= 767.98,
            isTablet: window.innerWidth > 767.98 && window.innerWidth <= 991.98,
            isDesktop: window.innerWidth > 991.98,
            width: window.innerWidth,
            height: window.innerHeight
        });

        onMounted(() => {
            // Add click handlers to all app links in the horizontal bar
            const appLinks = this.horizontalAppBar.el?.querySelectorAll('.horizontal-nav-link');
            if (appLinks) {
                appLinks.forEach(link => {
                    link.addEventListener('click', this.handleAppClick);
                });
            }

            // Detect and log viewport type on mount
            this.detectViewport();

            // Add resize listener to detect viewport changes
            window.addEventListener('resize', this.detectViewport);
        });

        onWillUnmount(() => {
            // Clean up resize listener to prevent memory leaks
            window.removeEventListener('resize', this.detectViewport);
        });
    },

    detectViewport() {
        const width = window.innerWidth;
        const height = window.innerHeight;
        const isMobile = width <= 767.98;
        const isTablet = width > 767.98 && width <= 991.98;
        const isDesktop = width > 991.98;

        // Update reactive state
        this.viewportState.isMobile = isMobile;
        this.viewportState.isTablet = isTablet;
        this.viewportState.isDesktop = isDesktop;
        this.viewportState.width = width;
        this.viewportState.height = height;

        // Log viewport information
        console.log('=== Vista Theme Viewport Detection ===');
        console.log(`Screen Width: ${width}px`);
        console.log(`Screen Height: ${height}px`);
        
        if (isMobile) {
            console.log('View Type: 📱 MOBILE/PHONE');
            console.log('Menu Style: More Menu button (dropdown)');
        } else if (isTablet) {
            console.log('View Type: 📱 TABLET');
            console.log('Menu Style: Direct sections');
        } else {
            console.log('View Type: 🖥️ DESKTOP');
            console.log('Menu Style: Full horizontal menu');
        }
        
        console.log('Breakpoints:');
        console.log('  - Mobile: ≤ 767.98px');
        console.log('  - Tablet: 768px - 991.98px');
        console.log('  - Desktop: ≥ 992px');
        console.log('=====================================');

        // Add data attribute to body for CSS targeting
        document.body.setAttribute('data-viewport-type', 
            isMobile ? 'mobile' : isTablet ? 'tablet' : 'desktop'
        );
    },

    handleAppClick(event) {
        // Remove active class from all links
        const allLinks = this.horizontalAppBar.el?.querySelectorAll('.horizontal-nav-link');
        if (allLinks) {
            allLinks.forEach(link => {
                link.classList.remove('active');
            });
        }
        
        // Add active class to clicked link
        event.currentTarget.classList.add('active');
        
        // Handle the app selection
        const li = event.currentTarget.parentElement;
        const a = event.currentTarget;
        const href = a.getAttribute('href');
        const id = href ? href.split('=')[1] : null;
        
        if (id) {
            document.querySelector('header').className = id;
        }
    }
})
